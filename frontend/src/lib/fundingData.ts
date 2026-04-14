import * as XLSX from 'xlsx';

import { supabase } from './supabase';

type UploadMode = 'excel' | 'csv';
type DecisionStatus = 'pending' | 'approved' | 'declined' | 'manual_review';

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
type SourceRow = Record<string, JsonValue>;

type SearchRecord = {
  ACCOUNT_NUMBER?: string | null;
  CUSTOMER_NAME1?: string | null;
  EC_NUMBER?: string | null;
  CUSTOMER_NO?: string | number | null;
  AMOUNT_FINANCED?: number | null;
  BOOK_DATE?: string | null;
};

type ReviewMatchRecord = SearchRecord & Record<string, string | number | null | undefined>;

type ReviewRecord = {
  application_id: string;
  row: number;
  applicant_name?: string | null;
  ec_number?: string | number | null;
  customer_no?: string | number | null;
  amount?: number | null;
  application_book_date?: string | null;
  category: 'anomaly' | 'clear';
  reason: string;
  anomaly_reasons?: string[];
  reference_date?: string | null;
  history_match_count: number;
  recent_match_count: number;
  latest_book_date?: string | null;
  matched_records: ReviewMatchRecord[];
  decision_status: DecisionStatus;
  response_status?: string | null;
};

type AnalysisResponse = {
  session_id: string;
  analysis_mode: UploadMode;
  file_name: string;
  total_processed: number;
  actionable_records: number;
  window_days: number;
  approved_count: number;
  rejected_count: number;
  pending_count: number;
  history_warning?: string | null;
  anomalies: ReviewRecord[];
  clear_records: ReviewRecord[];
};

type DecisionSummary = {
  approved_count: number;
  rejected_count: number;
  pending_count: number;
};

type DecisionResponse = {
  record: ReviewRecord;
  summary: DecisionSummary;
};

type ActivityEvent = {
  event_id: string;
  timestamp?: string | null;
  type: string;
  message: string;
  application_id?: string | null;
  record_label?: string | null;
  from_status?: string | null;
  to_status?: string | null;
  reason?: string | null;
  response_status?: string | null;
};

type ActivitySessionSummary = {
  session_id: string;
  analysis_mode: string;
  file_name: string;
  uploaded_at?: string | null;
  updated_at?: string | null;
  total_processed: number;
  actionable_records: number;
  record_count: number;
  approved_count: number;
  rejected_count: number;
  pending_count: number;
  event_count: number;
  history_warning?: string | null;
};

type ActivityRecord = ReviewRecord & {
  source_row: Record<string, string | number | boolean | null>;
  decision_history: ActivityEvent[];
};

type ActivitySessionDetail = ActivitySessionSummary & {
  columns: string[];
  events: ActivityEvent[];
  records: ActivityRecord[];
};

type ApprovalLedgerRecord = ReviewRecord & {
  session_id: string;
  file_name?: string | null;
  analysis_mode?: string | null;
  uploaded_at?: string | null;
  updated_at?: string | null;
  latest_activity_at?: string | null;
  approved_at?: string | null;
};

type ApprovalLedgerResponse = {
  window_days: number;
  generated_at: string;
  session_count: number;
  record_count: number;
  approved_count: number;
  rejected_count: number;
  pending_count: number;
  approved_amount: number;
  average_amount: number;
  flagged_approved_count: number;
  clear_approved_count: number;
  records: ApprovalLedgerRecord[];
};

type ReviewSessionRow = {
  id: string;
  analysis_mode: UploadMode;
  file_name: string;
  columns: string[] | null;
  total_processed: number;
  actionable_records: number;
  approved_count: number;
  rejected_count: number;
  pending_count: number;
  event_count: number;
  history_warning: string | null;
  uploaded_at: string;
  updated_at: string;
};

type ReviewRecordRow = {
  session_id: string;
  application_id: string;
  row_number: number;
  applicant_name: string | null;
  ec_number: string | null;
  customer_no: string | null;
  amount: number | null;
  application_book_date: string | null;
  category: 'anomaly' | 'clear';
  reason: string;
  anomaly_reasons: string[] | null;
  reference_date: string | null;
  history_match_count: number;
  recent_match_count: number;
  latest_book_date: string | null;
  matched_records: ReviewMatchRecord[] | null;
  decision_status: DecisionStatus;
  response_status: string | null;
  source_row: SourceRow | null;
  created_at: string;
  updated_at: string;
};

type ActivityEventRow = {
  id: string;
  session_id: string;
  application_id: string | null;
  event_type: string;
  record_label: string | null;
  from_status: string | null;
  to_status: string | null;
  message: string;
  reason: string | null;
  response_status: string | null;
  created_at: string;
};

type HistoryRecordRow = {
  id: string;
  source_session_id: string | null;
  source_application_id: string | null;
  import_row_number: number | null;
  account_number: string | null;
  customer_name1: string | null;
  ec_number: string | null;
  customer_no: string | null;
  amount_financed: number | null;
  book_date: string | null;
  normalized_ec_number: string | null;
  normalized_customer_no: string | null;
  row_data: SourceRow | null;
  created_at: string;
};

type PreparedHistoryRow = {
  id: string;
  bookDate: Date | null;
  ecKey: string | null;
  customerKey: string | null;
  ecNumber: string | null;
  customerNo: string | null;
  customerName1: string | null;
  displayRecord: ReviewMatchRecord;
};

type ParsedUpload = {
  columns: string[];
  rows: SourceRow[];
  totalProcessed: number;
};

const RECENT_APPLICATION_WINDOW_DAYS = 14;
const HISTORY_FETCH_CHUNK = 250;
const sessionSourceRowCache = new Map<string, Record<string, SourceRow>>();

function fail(message: string): never {
  throw new Error(message);
}

function normalizeScalar(value: unknown): string | number | boolean | Date | null {
  if (value === undefined || value === null) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'boolean') return value;
  return null;
}

function normalizeColumnName(columnName: unknown) {
  return String(columnName ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function normalizeIdentifier(value: unknown) {
  const normalized = normalizeScalar(value);
  if (normalized === null) return null;
  if (normalized instanceof Date) return normalized.toISOString();
  let text = typeof normalized === 'number' && Number.isInteger(normalized)
    ? String(Math.trunc(normalized))
    : String(normalized).trim().toUpperCase();
  if (text.endsWith('.0') && /^\d+\.0$/.test(text)) text = text.slice(0, -2);
  text = text.replace(/\s+/g, '');
  return text || null;
}

function normalizeName(name: unknown) {
  const normalized = normalizeScalar(name);
  if (normalized === null) return '';
  if (normalized instanceof Date) return normalized.toISOString().toUpperCase();
  return String(normalized).toUpperCase().split(/\s+/).filter(Boolean).join(' ');
}

function normalizeAmount(value: unknown) {
  const normalized = normalizeScalar(value);
  if (normalized === null) return null;
  if (typeof normalized === 'number') return Number.isFinite(normalized) ? normalized : null;
  if (typeof normalized !== 'string') return null;

  const sanitized = normalized.replace(/,/g, '').trim();
  if (!sanitized) return null;

  const parsed = Number(sanitized);
  return Number.isFinite(parsed) ? parsed : null;
}

function longestCommonSubstring(left: string, right: string) {
  const rows = Array.from({ length: left.length + 1 }, () => Array(right.length + 1).fill(0));
  let longest = 0;
  for (let i = 1; i <= left.length; i += 1) {
    for (let j = 1; j <= right.length; j += 1) {
      if (left[i - 1] === right[j - 1]) {
        rows[i][j] = rows[i - 1][j - 1] + 1;
        longest = Math.max(longest, rows[i][j]);
      }
    }
  }
  return longest;
}

function editDistance(left: string, right: string) {
  const rows = Array.from({ length: left.length + 1 }, () => Array(right.length + 1).fill(0));
  for (let i = 0; i <= left.length; i += 1) rows[i][0] = i;
  for (let j = 0; j <= right.length; j += 1) rows[0][j] = j;

  for (let i = 1; i <= left.length; i += 1) {
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      rows[i][j] = Math.min(rows[i - 1][j] + 1, rows[i][j - 1] + 1, rows[i - 1][j - 1] + cost);
    }
  }
  return rows[left.length][right.length];
}

function namesAreSimilar(leftName: unknown, rightName: unknown, threshold = 0.75) {
  const left = normalizeName(leftName);
  const right = normalizeName(rightName);
  if (left === right) return true;
  if (!left || !right) return false;

  const leftWords = new Set(left.split(' '));
  const rightWords = new Set(right.split(' '));
  const leftSubset = [...leftWords].every(word => rightWords.has(word));
  const rightSubset = [...rightWords].every(word => leftWords.has(word));
  if (leftSubset || rightSubset) return true;

  const overlap = [...leftWords].filter(word => rightWords.has(word)).length;
  const shorterWords = Math.min(leftWords.size, rightWords.size);
  if (shorterWords > 0 && overlap / shorterWords >= 0.6) return true;

  const shorter = left.length <= right.length ? left : right;
  const longer = left.length > right.length ? left : right;
  if (longer.length - shorter.length > Math.max(2, shorter.length * 0.2)) return false;

  const lcsSimilarity = longestCommonSubstring(left, right) / shorter.length;
  const editSimilarity = 1 - (editDistance(left, right) / Math.max(left.length, right.length));
  return Math.max(lcsSimilarity, editSimilarity) >= threshold;
}

function detectSimilarNames(names: string[]) {
  const filtered = names.filter(Boolean);
  for (let i = 0; i < filtered.length; i += 1) {
    for (let j = i + 1; j < filtered.length; j += 1) {
      if (namesAreSimilar(filtered[i], filtered[j])) return true;
    }
  }
  return false;
}

function excelSerialToDate(value: number) {
  const utcDays = Math.floor(value - 25569);
  const utcValue = utcDays * 86400;
  const fractionalDay = value - Math.floor(value) + 0.0000001;
  const totalSeconds = Math.floor(86400 * fractionalDay);
  const seconds = totalSeconds % 60;
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor(totalSeconds / 60) % 60;
  const date = new Date(utcValue * 1000);
  date.setUTCHours(hours, minutes, seconds, 0);
  return date;
}

function parseDate(value: unknown) {
  const normalized = normalizeScalar(value);
  if (normalized === null) return null;
  if (normalized instanceof Date) return normalized;
  if (typeof normalized === 'number') {
    const excelDate = excelSerialToDate(normalized);
    return Number.isNaN(excelDate.getTime()) ? null : excelDate;
  }
  const parsed = new Date(String(normalized));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toIsoDateTime(value: unknown) {
  const parsed = parseDate(value);
  return parsed ? parsed.toISOString() : null;
}

function makeJsonSafe(value: unknown): JsonValue {
  if (Array.isArray(value)) return value.map(item => makeJsonSafe(item));
  if (value && typeof value === 'object' && !(value instanceof Date)) {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [String(key), makeJsonSafe(item)]),
    );
  }
  if (value instanceof Date) return value.toISOString();
  const normalized = normalizeScalar(value);
  if (normalized instanceof Date) return normalized.toISOString();
  return (normalized as JsonPrimitive) ?? null;
}

function getFirstPresent(row: SourceRow, ...candidateNames: string[]) {
  const normalizedColumns = new Map(Object.keys(row).map(column => [normalizeColumnName(column), column] as const));
  for (const candidate of candidateNames) {
    const sourceColumn = normalizedColumns.get(normalizeColumnName(candidate));
    if (!sourceColumn) continue;
    const value = normalizeScalar(row[sourceColumn]);
    if (value !== null) return value;
  }
  return null;
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return [...new Set(values.filter((value): value is string => Boolean(value)))];
}

function buildApplicationRecord(row: SourceRow, index: number) {
  const applicationBookDate = parseDate(
    getFirstPresent(
      row,
      'BOOK_DATE',
      'BOOK DATE',
      'APPLICATION DATE',
      'DATE',
      'LOAN DATE',
      'DISBURSEMENT DATE',
      'APPROVAL DATE',
    ),
  );

  return {
    application_id: `row-${index + 2}`,
    row: index + 2,
    applicant_name: getFirstPresent(row, 'CUSTOMER_NAME1', 'CUSTOMER NAME1', 'CUSTOMER NAME', 'FULL NAME', 'NAME', 'APPLICANT NAME', 'CLIENT NAME', 'BORROWER NAME') as string | null,
    ec_number: getFirstPresent(row, 'EC_NUMBER', 'EC NUMBER', 'EC NO', 'EC', 'ECONOMIC CENTER', 'ECONOMIC CENTRE', 'BRANCH CODE', 'BRANCH') as string | number | null,
    customer_no: getFirstPresent(row, 'CUSTOMER_NO', 'CUSTOMER NO', 'ID', 'ID NUMBER', 'CUSTOMER ID', 'CLIENT ID', 'ACCOUNT NUMBER', 'ACCOUNT NO', 'ACCOUNT') as string | number | null,
    amount: normalizeAmount(getFirstPresent(row, 'AMOUNT_FINANCED', 'AMOUNT FINANCED', 'AMOUNT', 'LOAN AMOUNT', 'FINANCE AMOUNT', 'CREDIT AMOUNT', 'VALUE')),
    application_book_date: applicationBookDate?.toISOString() ?? null,
    decision_status: 'pending' as const,
  };
}

function toMatchDisplayRecord(row: HistoryRecordRow): ReviewMatchRecord {
  const base = (row.row_data ?? {}) as ReviewMatchRecord;
  return {
    ...base,
    ACCOUNT_NUMBER: (base.ACCOUNT_NUMBER ?? row.account_number ?? null) as string | null,
    CUSTOMER_NAME1: (base.CUSTOMER_NAME1 ?? row.customer_name1 ?? null) as string | null,
    EC_NUMBER: (base.EC_NUMBER ?? row.ec_number ?? null) as string | null,
    CUSTOMER_NO: (base.CUSTOMER_NO ?? row.customer_no ?? null) as string | number | null,
    AMOUNT_FINANCED: (base.AMOUNT_FINANCED ?? row.amount_financed ?? null) as number | null,
    BOOK_DATE: (base.BOOK_DATE ?? row.book_date ?? null) as string | null,
  };
}

function prepareHistoryRows(historyRows: HistoryRecordRow[]) {
  const preparedRows: PreparedHistoryRow[] = historyRows.map(row => ({
    id: row.id,
    bookDate: parseDate(row.book_date ?? (row.row_data ?? {}).BOOK_DATE),
    ecKey: row.normalized_ec_number ?? normalizeIdentifier(row.ec_number ?? (row.row_data ?? {}).EC_NUMBER),
    customerKey: row.normalized_customer_no ?? normalizeIdentifier(row.customer_no ?? (row.row_data ?? {}).CUSTOMER_NO),
    ecNumber: row.ec_number ?? ((row.row_data ?? {}).EC_NUMBER as string | null) ?? null,
    customerNo: row.customer_no ?? ((row.row_data ?? {}).CUSTOMER_NO as string | null) ?? null,
    customerName1: row.customer_name1 ?? ((row.row_data ?? {}).CUSTOMER_NAME1 as string | null) ?? null,
    displayRecord: toMatchDisplayRecord(row),
  }));

  const lookup = new Map<string, number[]>();
  preparedRows.forEach((row, index) => {
    for (const key of [row.ecKey, row.customerKey]) {
      if (!key) continue;
      const indices = lookup.get(key) ?? [];
      indices.push(index);
      lookup.set(key, indices);
    }
  });

  return { preparedRows, lookup };
}

function analyzeApplication(row: SourceRow, index: number, historyRows: PreparedHistoryRow[], historyLookup: Map<string, number[]>) {
  const application = buildApplicationRecord(row, index);
  const identifierKeys = uniqueStrings([normalizeIdentifier(application.ec_number), normalizeIdentifier(application.customer_no)]);
  const referenceDate = parseDate(application.application_book_date) ?? new Date();
  const reviewWindowStart = new Date(referenceDate.getTime() - RECENT_APPLICATION_WINDOW_DAYS * 24 * 60 * 60 * 1000);
  const anomalyReasons: string[] = [];

  if (identifierKeys.length === 0) {
    return {
      ...application,
      category: 'anomaly' as const,
      anomaly_reasons: ['Missing EC number / customer ID. Unable to check recent history.'],
      reason: 'Missing EC number / customer ID. Unable to check recent history.',
      reference_date: referenceDate.toISOString(),
      history_match_count: 0,
      recent_match_count: 0,
      latest_book_date: null,
      matched_records: [],
      response_status: null,
    } satisfies ReviewRecord;
  }

  const matchedIndices = new Set<number>();
  identifierKeys.forEach(key => (historyLookup.get(key) ?? []).forEach(indexValue => matchedIndices.add(indexValue)));
  const matchedRows = [...matchedIndices]
    .map(matchIndex => historyRows[matchIndex])
    .sort((left, right) => (right.bookDate?.getTime() ?? 0) - (left.bookDate?.getTime() ?? 0));

  const recentMatches = matchedRows.filter(historyRow => historyRow.bookDate !== null && historyRow.bookDate >= reviewWindowStart && historyRow.bookDate <= referenceDate);
  const latestBookDate = matchedRows.find(historyRow => historyRow.bookDate)?.bookDate ?? null;

  if (recentMatches.length > 0) anomalyReasons.push(`Previous loan found within the last ${RECENT_APPLICATION_WINDOW_DAYS} days.`);

  if (matchedRows.length > 0) {
    const currentName = normalizeName(application.applicant_name);
    const appEcKey = normalizeIdentifier(application.ec_number);
    const appCustomerKey = normalizeIdentifier(application.customer_no);

    const ecGroups = new Map<string, PreparedHistoryRow[]>();
    matchedRows.forEach(historyRow => {
      if (!historyRow.ecKey) return;
      const existing = ecGroups.get(historyRow.ecKey) ?? [];
      existing.push(historyRow);
      ecGroups.set(historyRow.ecKey, existing);
    });

    for (const [ecKey, group] of ecGroups.entries()) {
      const originalEc = group[0]?.ecNumber;
      const allNames = [...uniqueStrings(group.map(item => normalizeName(item.customerName1)))];
      if (appEcKey === ecKey && currentName && !allNames.includes(currentName)) allNames.push(currentName);
      if (allNames.length > 1) {
        anomalyReasons.push(
          detectSimilarNames(allNames)
            ? `Potential identity fraud: EC number '${originalEc}' used by similar names (possible typos/variations).`
            : `Identity fraud detected: EC number '${originalEc}' used by ${allNames.length} different names.`,
        );
        break;
      }
    }

    if (!anomalyReasons.some(reason => reason.includes('Identity fraud detected') || reason.includes('Potential identity fraud'))) {
      const customerGroups = new Map<string, PreparedHistoryRow[]>();
      matchedRows.forEach(historyRow => {
        if (!historyRow.customerKey) return;
        const existing = customerGroups.get(historyRow.customerKey) ?? [];
        existing.push(historyRow);
        customerGroups.set(historyRow.customerKey, existing);
      });

      for (const [customerKey, group] of customerGroups.entries()) {
        const originalCustomer = group[0]?.customerNo;
        const allNames = [...uniqueStrings(group.map(item => normalizeName(item.customerName1)))];
        if (appCustomerKey === customerKey && currentName && !allNames.includes(currentName)) allNames.push(currentName);
        if (allNames.length > 1) {
          anomalyReasons.push(
            detectSimilarNames(allNames)
              ? `Potential identity fraud: Account number '${originalCustomer}' used by similar names (possible typos/variations).`
              : `Identity fraud detected: Account number '${originalCustomer}' used by ${allNames.length} different names.`,
          );
          break;
        }
      }
    }
  }

  return {
    ...application,
    category: anomalyReasons.length > 0 ? 'anomaly' : 'clear',
    anomaly_reasons: anomalyReasons,
    reason: anomalyReasons.length > 0
      ? anomalyReasons.join(' | ')
      : matchedRows.length > 0
        ? `History exists (${matchedRows.length} records), no recent conflicts.`
        : 'No matching history found.',
    reference_date: referenceDate.toISOString(),
    history_match_count: matchedRows.length,
    recent_match_count: recentMatches.length,
    latest_book_date: latestBookDate?.toISOString() ?? null,
    matched_records: (recentMatches.length > 0 ? recentMatches : matchedRows).slice(0, 5).map(item => item.displayRecord),
    response_status: null,
  } satisfies ReviewRecord;
}

function determineInitialDecisionStatus(row: SourceRow): DecisionStatus | null {
  const rawResponse = getFirstPresent(row, 'RESPONSE', 'STATUS', 'DECISION', 'RESULT', 'APPLICATION_STATUS', 'RESPONSE_STATUS', 'BeneficiaryStatus');
  if (rawResponse === null) return null;
  const response = String(rawResponse).trim().toLowerCase();
  if (!response) return null;
  if (['accept', 'approve', 'yes', 'true', '1', 'ok', 'passed', 'completed', 'in process'].some(token => response.includes(token))) return 'approved';
  if (['reject', 'decline', 'deny', 'no', 'false', '0', 'failed', 'denied'].some(token => response.includes(token))) return 'declined';
  if (['review', 'hold', 'pending'].some(token => response.includes(token))) return 'manual_review';
  return null;
}

function buildCsvResponseRecord(row: SourceRow, index: number) {
  const initialStatus = determineInitialDecisionStatus(row);
  const responseStatus = getFirstPresent(row, 'BeneficiaryStatus', 'Status', 'STATUS', 'DECISION', 'RESULT', 'APPLICATION_STATUS');

  return {
    application_id: `row-${index + 2}`,
    row: index + 2,
    applicant_name: getFirstPresent(row, 'BeneficiaryName', 'BENEFICIARY NAME', 'NAME', 'APPLICANT NAME') as string | null,
    ec_number: getFirstPresent(row, 'Reference', 'REFERENCE', 'REF', 'ID') as string | number | null,
    customer_no: getFirstPresent(row, 'Reference', 'REFERENCE', 'REF', 'ID') as string | number | null,
    amount: normalizeAmount(getFirstPresent(row, 'Amount', 'AMOUNT', 'AMOUNT_FINANCED', 'LOAN AMOUNT')),
    application_book_date: null,
    category: 'clear' as const,
    reason: initialStatus !== null ? `CSV response: ${String(responseStatus ?? '')}.` : 'CSV response pending review.',
    anomaly_reasons: [],
    reference_date: new Date().toISOString(),
    history_match_count: 0,
    recent_match_count: 0,
    latest_book_date: null,
    matched_records: [],
    decision_status: initialStatus ?? 'pending',
    response_status: normalizeScalar(responseStatus) as string | null,
  } satisfies ReviewRecord;
}

function computeSummary(records: ReviewRecord[]): DecisionSummary {
  return {
    approved_count: records.filter(record => record.decision_status === 'approved').length,
    rejected_count: records.filter(record => record.decision_status === 'declined').length,
    pending_count: records.filter(record => record.decision_status === 'pending' || record.decision_status === 'manual_review').length,
  };
}

function buildSessionResponse(sessionId: string, mode: UploadMode, fileName: string, totalProcessed: number, records: ReviewRecord[]): AnalysisResponse {
  const summary = computeSummary(records);
  return {
    session_id: sessionId,
    analysis_mode: mode,
    file_name: fileName,
    total_processed: totalProcessed,
    actionable_records: records.length,
    window_days: RECENT_APPLICATION_WINDOW_DAYS,
    ...summary,
    history_warning: null,
    anomalies: records.filter(record => record.category === 'anomaly'),
    clear_records: records.filter(record => record.category === 'clear'),
  };
}

function recordLabel(record: ReviewRecord) {
  return String(record.applicant_name ?? record.ec_number ?? record.customer_no ?? record.application_id);
}

function buildRecordEvent(record: ReviewRecord, eventType: string, timestamp: string, overrides?: Partial<ActivityEvent>) {
  const targetStatus = overrides?.to_status ?? record.decision_status;
  const defaultMessage = eventType === 'record_ingested'
    ? `Record ingested with initial status '${targetStatus}'.`
    : eventType === 'record_decision_updated' && overrides?.from_status
      ? `Decision changed from '${overrides.from_status}' to '${targetStatus}'.`
      : eventType.replace(/_/g, ' ');
  return {
    event_id: crypto.randomUUID(),
    timestamp,
    type: eventType,
    application_id: record.application_id,
    record_label: recordLabel(record),
    from_status: overrides?.from_status ?? null,
    to_status: targetStatus ?? null,
    message: overrides?.message ?? defaultMessage,
    reason: overrides?.reason ?? record.reason ?? null,
    response_status: overrides?.response_status ?? record.response_status ?? null,
  } satisfies ActivityEvent;
}

function chunkValues<T>(values: T[], chunkSize: number) {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) chunks.push(values.slice(index, index + chunkSize));
  return chunks;
}

async function runQuery<T>(promise: PromiseLike<{ data: T | null; error: { message: string } | null }>, fallback: string) {
  const { data, error } = await promise;
  if (error) fail(error.message || fallback);
  return data;
}

function mapReviewRecordRow(row: ReviewRecordRow): ReviewRecord {
  return {
    application_id: row.application_id,
    row: row.row_number,
    applicant_name: row.applicant_name,
    ec_number: row.ec_number,
    customer_no: row.customer_no,
    amount: row.amount,
    application_book_date: row.application_book_date,
    category: row.category,
    reason: row.reason,
    anomaly_reasons: row.anomaly_reasons ?? [],
    reference_date: row.reference_date,
    history_match_count: row.history_match_count,
    recent_match_count: row.recent_match_count,
    latest_book_date: row.latest_book_date,
    matched_records: row.matched_records ?? [],
    decision_status: row.decision_status,
    response_status: row.response_status,
  };
}

function mapActivityEventRow(row: ActivityEventRow): ActivityEvent {
  return {
    event_id: row.id,
    timestamp: row.created_at,
    type: row.event_type,
    message: row.message,
    application_id: row.application_id,
    record_label: row.record_label,
    from_status: row.from_status,
    to_status: row.to_status,
    reason: row.reason,
    response_status: row.response_status,
  };
}

function mapSessionSummary(row: ReviewSessionRow): ActivitySessionSummary {
  return {
    session_id: row.id,
    analysis_mode: row.analysis_mode,
    file_name: row.file_name,
    uploaded_at: row.uploaded_at,
    updated_at: row.updated_at,
    total_processed: row.total_processed,
    actionable_records: row.actionable_records,
    record_count: row.actionable_records,
    approved_count: row.approved_count,
    rejected_count: row.rejected_count,
    pending_count: row.pending_count,
    event_count: row.event_count,
    history_warning: row.history_warning,
  };
}

function sanitizeLikeQuery(query: string) {
  return query.replace(/[%_,()]/g, '');
}

function parseMatrixRows(matrix: unknown[][], headerRowIndex: number): ParsedUpload {
  const headerRow = matrix[headerRowIndex] ?? [];
  const columns = headerRow.map((value, index) => String(normalizeScalar(value) ?? `COLUMN_${index + 1}`));
  const dataRows = matrix.slice(headerRowIndex + 1);
  const rows = dataRows
    .map(rawRow => Object.fromEntries(columns.map((column, index) => [column, makeJsonSafe(rawRow[index])])) as SourceRow)
    .filter(row => Object.values(row).some(value => value !== null && value !== ''));

  return {
    columns,
    rows,
    totalProcessed: rows.length,
  };
}

async function parseExcelUpload(file: File): Promise<ParsedUpload> {
  const workbook = XLSX.read(await file.arrayBuffer(), { type: 'array', cellDates: true });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const matrix = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: true, defval: null, blankrows: false });
  return parseMatrixRows(matrix, 1);
}

async function parseCsvUpload(file: File): Promise<ParsedUpload> {
  const workbook = XLSX.read(await file.text(), { type: 'string', raw: true, cellDates: true });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const matrix = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: true, defval: null, blankrows: false });
  return parseMatrixRows(matrix, 0);
}

async function fetchHistoryRowsForUpload(rows: SourceRow[]) {
  const ecKeys = uniqueStrings(rows.map(row => normalizeIdentifier(getFirstPresent(row, 'EC_NUMBER', 'EC NUMBER', 'EC NO', 'EC', 'ECONOMIC CENTER', 'ECONOMIC CENTRE', 'BRANCH CODE', 'BRANCH'))));
  const customerKeys = uniqueStrings(rows.map(row => normalizeIdentifier(getFirstPresent(row, 'CUSTOMER_NO', 'CUSTOMER NO', 'ID', 'ID NUMBER', 'CUSTOMER ID', 'CLIENT ID', 'ACCOUNT NUMBER', 'ACCOUNT NO', 'ACCOUNT'))));
  const results = new Map<string, HistoryRecordRow>();
  const selectColumns = 'id, source_session_id, source_application_id, import_row_number, account_number, customer_name1, ec_number, customer_no, amount_financed, book_date, normalized_ec_number, normalized_customer_no, row_data, created_at';

  for (const chunk of chunkValues(ecKeys, HISTORY_FETCH_CHUNK)) {
    const data = await runQuery(
      supabase.from('history_records').select(selectColumns).in('normalized_ec_number', chunk),
      'Failed to load history records by EC number.',
    );
    (data ?? []).forEach(row => results.set((row as HistoryRecordRow).id, row as HistoryRecordRow));
  }

  for (const chunk of chunkValues(customerKeys, HISTORY_FETCH_CHUNK)) {
    const data = await runQuery(
      supabase.from('history_records').select(selectColumns).in('normalized_customer_no', chunk),
      'Failed to load history records by customer number.',
    );
    (data ?? []).forEach(row => results.set((row as HistoryRecordRow).id, row as HistoryRecordRow));
  }

  return [...results.values()];
}

function extractHistoryColumns(row: SourceRow) {
  return {
    account_number: getFirstPresent(row, 'ACCOUNT_NUMBER', 'ACCOUNT NUMBER', 'ACCOUNT NO', 'ACCOUNT', 'CUSTOMER_NO', 'CUSTOMER NO'),
    customer_name1: getFirstPresent(row, 'CUSTOMER_NAME1', 'CUSTOMER NAME1', 'CUSTOMER NAME', 'FULL NAME', 'NAME', 'APPLICANT NAME'),
    ec_number: getFirstPresent(row, 'EC_NUMBER', 'EC NUMBER', 'EC NO', 'EC', 'BRANCH CODE', 'BRANCH'),
    customer_no: getFirstPresent(row, 'CUSTOMER_NO', 'CUSTOMER NO', 'ID', 'ID NUMBER', 'ACCOUNT NUMBER', 'ACCOUNT NO', 'ACCOUNT'),
    amount_financed: getFirstPresent(row, 'AMOUNT_FINANCED', 'AMOUNT FINANCED', 'AMOUNT', 'LOAN AMOUNT', 'FINANCE AMOUNT', 'VALUE'),
    book_date: getFirstPresent(row, 'BOOK_DATE', 'BOOK DATE', 'APPLICATION DATE', 'DATE', 'LOAN DATE', 'DISBURSEMENT DATE', 'APPROVAL DATE'),
  };
}

async function persistSession(
  sessionId: string,
  mode: UploadMode,
  fileName: string,
  columns: string[],
  totalProcessed: number,
  records: ReviewRecord[],
  rowsById: Record<string, SourceRow>,
) {
  const now = new Date().toISOString();
  const summary = computeSummary(records);

  await runQuery(
    supabase.from('review_sessions').insert({
      id: sessionId,
      analysis_mode: mode,
      file_name: fileName,
      columns,
      total_processed: totalProcessed,
      actionable_records: records.length,
      approved_count: summary.approved_count,
      rejected_count: summary.rejected_count,
      pending_count: summary.pending_count,
      event_count: 1,
      history_warning: null,
      uploaded_at: now,
      updated_at: now,
    }),
    'Failed to create the review session.',
  );

  await runQuery(
    supabase.from('review_records').insert(records.map(record => ({
      session_id: sessionId,
      application_id: record.application_id,
      row_number: record.row,
      applicant_name: record.applicant_name ?? null,
      ec_number: record.ec_number ? String(record.ec_number) : null,
      customer_no: record.customer_no ? String(record.customer_no) : null,
      amount: typeof record.amount === 'number' ? record.amount : record.amount ? Number(record.amount) : null,
      application_book_date: record.application_book_date ?? null,
      category: record.category,
      reason: record.reason,
      anomaly_reasons: record.anomaly_reasons ?? [],
      reference_date: record.reference_date ?? null,
      history_match_count: record.history_match_count,
      recent_match_count: record.recent_match_count,
      latest_book_date: record.latest_book_date ?? null,
      matched_records: record.matched_records,
      decision_status: record.decision_status,
      response_status: record.response_status ?? null,
      source_row: rowsById[record.application_id] ?? {},
      created_at: now,
      updated_at: now,
    }))),
    'Failed to create review records.',
  );

  await runQuery(
    supabase.from('activity_events').insert([
      {
        session_id: sessionId,
        application_id: null,
        event_type: 'upload_created',
        record_label: null,
        from_status: null,
        to_status: null,
        message: `Uploaded ${fileName} for ${mode} analysis.`,
        reason: null,
        response_status: null,
        created_at: now,
      },
      ...records.map(record => {
        const event = buildRecordEvent(record, 'record_ingested', now);
        return {
          session_id: sessionId,
          application_id: record.application_id,
          event_type: event.type,
          record_label: event.record_label ?? null,
          from_status: event.from_status ?? null,
          to_status: event.to_status ?? null,
          message: event.message,
          reason: event.reason ?? null,
          response_status: event.response_status ?? null,
          created_at: now,
        };
      }),
    ]),
    'Failed to create activity events.',
  );
}

async function syncSummaryCounts(sessionId: string) {
  const recordRows = await runQuery(
    supabase.from('review_records').select('decision_status').eq('session_id', sessionId),
    'Failed to refresh session counts.',
  );
  const summary = computeSummary(
    (recordRows ?? []).map(row => ({
      application_id: '',
      row: 0,
      category: 'clear',
      reason: '',
      history_match_count: 0,
      recent_match_count: 0,
      matched_records: [],
      decision_status: (row as { decision_status: DecisionStatus }).decision_status,
    }) as ReviewRecord),
  );

  const sessionEventRows = await runQuery(
    supabase.from('activity_events').select('id').eq('session_id', sessionId).is('application_id', null),
    'Failed to refresh session event counts.',
  );

  const updatedAt = new Date().toISOString();
  await runQuery(
    supabase.from('review_sessions').update({
      approved_count: summary.approved_count,
      rejected_count: summary.rejected_count,
      pending_count: summary.pending_count,
      event_count: (sessionEventRows ?? []).length,
      updated_at: updatedAt,
    }).eq('id', sessionId),
    'Failed to update the session summary.',
  );

  return summary;
}

async function fetchSessionRecordRow(sessionId: string, applicationId: string): Promise<ReviewRecordRow> {
  const data = await runQuery(
    supabase
      .from('review_records')
      .select('session_id, application_id, row_number, applicant_name, ec_number, customer_no, amount, application_book_date, category, reason, anomaly_reasons, reference_date, history_match_count, recent_match_count, latest_book_date, matched_records, decision_status, response_status, source_row, created_at, updated_at')
      .eq('session_id', sessionId)
      .eq('application_id', applicationId)
      .single(),
    'Failed to load the selected record.',
  );
  if (!data) fail('Failed to load the selected record.');
  return data as ReviewRecordRow;
}

async function fetchSessionRow(sessionId: string): Promise<ReviewSessionRow> {
  const data = await runQuery(
    supabase
      .from('review_sessions')
      .select('id, analysis_mode, file_name, columns, total_processed, actionable_records, approved_count, rejected_count, pending_count, event_count, history_warning, uploaded_at, updated_at')
      .eq('id', sessionId)
      .single(),
    'Failed to load the review session.',
  );
  if (!data) fail('Failed to load the review session.');
  return data as ReviewSessionRow;
}

async function appendApprovedRecordToHistory(sessionId: string, recordRow: ReviewRecordRow) {
  const sourceRow = (recordRow.source_row ?? {}) as SourceRow;
  const extracted = extractHistoryColumns(sourceRow);
  await runQuery(
    supabase.from('history_records').insert({
      source_session_id: sessionId,
      source_application_id: recordRow.application_id,
      import_row_number: recordRow.row_number,
      account_number: extracted.account_number ? String(extracted.account_number) : null,
      customer_name1: extracted.customer_name1 ? String(extracted.customer_name1) : null,
      ec_number: extracted.ec_number ? String(extracted.ec_number) : null,
      customer_no: extracted.customer_no ? String(extracted.customer_no) : null,
      amount_financed: typeof extracted.amount_financed === 'number' ? extracted.amount_financed : extracted.amount_financed ? Number(extracted.amount_financed) : null,
      book_date: toIsoDateTime(extracted.book_date),
      normalized_ec_number: normalizeIdentifier(extracted.ec_number),
      normalized_customer_no: normalizeIdentifier(extracted.customer_no),
      row_data: sourceRow,
    }),
    'Failed to append the approved record to history.',
  );
}

export async function analyzeUpload(file: File, uploadMode: UploadMode): Promise<AnalysisResponse> {
  const parsed = uploadMode === 'excel' ? await parseExcelUpload(file) : await parseCsvUpload(file);
  const sessionId = crypto.randomUUID();
  const rowsById: Record<string, SourceRow> = {};

  let records: ReviewRecord[];
  if (uploadMode === 'excel') {
    const historyRows = await fetchHistoryRowsForUpload(parsed.rows);
    const { preparedRows, lookup } = prepareHistoryRows(historyRows);
    records = parsed.rows.map((row, index) => analyzeApplication(row, index, preparedRows, lookup));
  } else {
    records = parsed.rows.map((row, index) => buildCsvResponseRecord(row, index));
  }

  records.forEach((record, index) => {
    rowsById[record.application_id] = parsed.rows[index];
  });
  sessionSourceRowCache.set(sessionId, rowsById);

  await persistSession(sessionId, uploadMode, file.name, parsed.columns, parsed.totalProcessed, records, rowsById);
  return buildSessionResponse(sessionId, uploadMode, file.name, parsed.totalProcessed, records);
}

export async function updateRecordDecision(
  sessionId: string,
  applicationId: string,
  decision: DecisionStatus,
): Promise<DecisionResponse> {
  const session = await fetchSessionRow(sessionId);
  const recordRow = await fetchSessionRecordRow(sessionId, applicationId);

  if (decision === recordRow.decision_status) {
    return {
      record: mapReviewRecordRow(recordRow),
      summary: {
        approved_count: session.approved_count,
        rejected_count: session.rejected_count,
        pending_count: session.pending_count,
      },
    };
  }

  if (session.analysis_mode === 'excel') {
    if (recordRow.decision_status === 'approved' && decision !== 'approved') {
      fail('Approved records are already written to history and cannot be changed.');
    }
    if (recordRow.decision_status !== 'approved' && decision === 'approved') {
      await appendApprovedRecordToHistory(sessionId, recordRow);
    }
  }

  const updatedAt = new Date().toISOString();
  await runQuery(
    supabase
      .from('review_records')
      .update({ decision_status: decision, updated_at: updatedAt })
      .eq('session_id', sessionId)
      .eq('application_id', applicationId),
    'Failed to update the review record.',
  );

  const updatedRecordRow = await fetchSessionRecordRow(sessionId, applicationId);
  const updatedRecord = mapReviewRecordRow(updatedRecordRow);
  const historySynced = session.analysis_mode === 'excel' && recordRow.decision_status !== 'approved' && decision === 'approved';
  const recordEvent = buildRecordEvent(updatedRecord, 'record_decision_updated', updatedAt, {
    from_status: recordRow.decision_status,
    to_status: decision,
    message: historySynced ? `Decision changed from '${recordRow.decision_status}' to '${decision}'. Record synced to history.` : undefined,
  });

  await runQuery(
    supabase.from('activity_events').insert([
      {
        session_id: sessionId,
        application_id: applicationId,
        event_type: recordEvent.type,
        record_label: recordEvent.record_label ?? null,
        from_status: recordEvent.from_status ?? null,
        to_status: recordEvent.to_status ?? null,
        message: recordEvent.message,
        reason: recordEvent.reason ?? null,
        response_status: recordEvent.response_status ?? null,
        created_at: updatedAt,
      },
      {
        session_id: sessionId,
        application_id: null,
        event_type: 'record_decision_updated',
        record_label: recordLabel(updatedRecord),
        from_status: recordRow.decision_status,
        to_status: decision,
        message: `${recordLabel(updatedRecord)} moved from '${recordRow.decision_status}' to '${decision}'.`,
        reason: updatedRecord.reason ?? null,
        response_status: updatedRecord.response_status ?? null,
        created_at: updatedAt,
      },
    ]),
    'Failed to store activity events for the decision update.',
  );

  const summary = await syncSummaryCounts(sessionId);
  return { record: updatedRecord, summary };
}

function buildSheetBlob(rows: SourceRow[], bookType: 'xlsx' | 'csv') {
  const worksheet = XLSX.utils.json_to_sheet(rows);
  if (bookType === 'csv') {
    const csv = XLSX.utils.sheet_to_csv(worksheet);
    return new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  }

  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
  const buffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
  return new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
}

export async function downloadSessionResults(results: AnalysisResponse) {
  let rowsById = sessionSourceRowCache.get(results.session_id);
  if (!rowsById) {
    const recordRows = await runQuery(
      supabase.from('review_records').select('application_id, source_row').eq('session_id', results.session_id),
      'Failed to load downloadable rows for the current session.',
    );
    rowsById = Object.fromEntries(
      (recordRows ?? []).map(row => [
        (row as { application_id: string }).application_id,
        ((row as { source_row: SourceRow | null }).source_row ?? {}) as SourceRow,
      ]),
    );
    sessionSourceRowCache.set(results.session_id, rowsById);
  }

  const records = [...results.anomalies, ...results.clear_records];
  const selectedIds = records
    .filter(record => (results.analysis_mode === 'csv' ? record.decision_status === 'declined' : record.decision_status === 'approved'))
    .map(record => record.application_id);
  const rows = selectedIds.map(applicationId => rowsById?.[applicationId]).filter(Boolean) as SourceRow[];
  if (rows.length === 0) {
    fail(results.analysis_mode === 'csv' ? 'No rejected records available for download.' : 'No approved records available for download.');
  }

  const stem = results.file_name.replace(/\.[^.]+$/i, '');
  if (results.analysis_mode === 'csv') {
    return {
      blob: buildSheetBlob(rows, 'csv'),
      fileName: `${stem}_rejected.csv`,
    };
  }

  return {
    blob: buildSheetBlob(rows, 'xlsx'),
    fileName: `${stem}_approved.xlsx`,
  };
}

export async function replaceHistoryWithWorkbook(file: File) {
  const parsed = await parseExcelUpload(file);
  const rows = parsed.rows.map((row, index) => {
    const extracted = extractHistoryColumns(row);
    return {
      import_row_number: index + 2,
      account_number: extracted.account_number ? String(extracted.account_number) : null,
      customer_name1: extracted.customer_name1 ? String(extracted.customer_name1) : null,
      ec_number: extracted.ec_number ? String(extracted.ec_number) : null,
      customer_no: extracted.customer_no ? String(extracted.customer_no) : null,
      amount_financed: typeof extracted.amount_financed === 'number' ? extracted.amount_financed : extracted.amount_financed ? Number(extracted.amount_financed) : null,
      book_date: toIsoDateTime(extracted.book_date),
      normalized_ec_number: normalizeIdentifier(extracted.ec_number),
      normalized_customer_no: normalizeIdentifier(extracted.customer_no),
      row_data: row,
    };
  });

  await runQuery(
    supabase.from('history_records').delete().not('id', 'is', null),
    'Failed to clear the existing history table.',
  );

  for (const chunk of chunkValues(rows, 200)) {
    await runQuery(
      supabase.from('history_records').insert(chunk),
      'Failed to import history workbook rows into Supabase.',
    );
  }

  return rows.length;
}

export async function searchHistory(query: string): Promise<SearchRecord[]> {
  const trimmed = query.trim();
  if (!trimmed) return [];
  const safeQuery = sanitizeLikeQuery(trimmed);
  const pattern = `%${safeQuery}%`;
  const data = await runQuery(
    supabase
      .from('history_records')
      .select('account_number, customer_name1, ec_number, customer_no, amount_financed, book_date')
      .or(`ec_number.ilike.${pattern},customer_no.ilike.${pattern},customer_name1.ilike.${pattern}`)
      .order('book_date', { ascending: false })
      .limit(100),
    'Failed to search history.',
  );

  return (data ?? []).map(row => ({
    ACCOUNT_NUMBER: (row as HistoryRecordRow).account_number ?? null,
    CUSTOMER_NAME1: (row as HistoryRecordRow).customer_name1 ?? null,
    EC_NUMBER: (row as HistoryRecordRow).ec_number ?? null,
    CUSTOMER_NO: (row as HistoryRecordRow).customer_no ?? null,
    AMOUNT_FINANCED: (row as HistoryRecordRow).amount_financed ?? null,
    BOOK_DATE: (row as HistoryRecordRow).book_date ?? null,
  }));
}

export async function getActivitySessions(): Promise<ActivitySessionSummary[]> {
  const data = await runQuery(
    supabase
      .from('review_sessions')
      .select('id, analysis_mode, file_name, columns, total_processed, actionable_records, approved_count, rejected_count, pending_count, event_count, history_warning, uploaded_at, updated_at')
      .order('updated_at', { ascending: false }),
    'Failed to load activity sessions.',
  );
  return (data ?? []).map(row => mapSessionSummary(row as ReviewSessionRow));
}

export async function getActivitySessionDetail(sessionId: string): Promise<ActivitySessionDetail> {
  const session = await fetchSessionRow(sessionId);
  const [recordData, eventData] = await Promise.all([
    runQuery(
      supabase
        .from('review_records')
        .select('session_id, application_id, row_number, applicant_name, ec_number, customer_no, amount, application_book_date, category, reason, anomaly_reasons, reference_date, history_match_count, recent_match_count, latest_book_date, matched_records, decision_status, response_status, source_row, created_at, updated_at')
        .eq('session_id', sessionId)
        .order('row_number', { ascending: true }),
      'Failed to load session records.',
    ),
    runQuery(
      supabase
        .from('activity_events')
        .select('id, session_id, application_id, event_type, record_label, from_status, to_status, message, reason, response_status, created_at')
        .eq('session_id', sessionId)
        .order('created_at', { ascending: false }),
      'Failed to load activity events.',
    ),
  ]);

  const events = (eventData ?? []).map(row => mapActivityEventRow(row as ActivityEventRow));
  const recordEventsByApplicationId = new Map<string, ActivityEvent[]>();
  events.filter(event => event.application_id).forEach(event => {
    const applicationId = event.application_id as string;
    const existing = recordEventsByApplicationId.get(applicationId) ?? [];
    existing.push(event);
    recordEventsByApplicationId.set(applicationId, existing);
  });

  return {
    ...mapSessionSummary(session),
    columns: session.columns ?? [],
    events: events.filter(event => !event.application_id),
    records: (recordData ?? []).map(row => {
      const recordRow = row as ReviewRecordRow;
      return {
        ...mapReviewRecordRow(recordRow),
        source_row: (recordRow.source_row ?? {}) as Record<string, string | number | boolean | null>,
        decision_history: recordEventsByApplicationId.get(recordRow.application_id) ?? [],
      };
    }),
  };
}

export async function getApprovalLedger(days = RECENT_APPLICATION_WINDOW_DAYS): Promise<ApprovalLedgerResponse> {
  const windowDays = Math.min(Math.max(days, 1), 365);
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();
  const recordRows = await runQuery(
    supabase
      .from('review_records')
      .select('session_id, application_id, row_number, applicant_name, ec_number, customer_no, amount, application_book_date, category, reason, anomaly_reasons, reference_date, history_match_count, recent_match_count, latest_book_date, matched_records, decision_status, response_status, source_row, created_at, updated_at')
      .gte('updated_at', cutoff)
      .order('updated_at', { ascending: false }),
    'Failed to load the approval ledger.',
  );

  const allRecords = (recordRows ?? []).map(row => row as ReviewRecordRow);
  const sessionIds = uniqueStrings(allRecords.map(row => row.session_id));
  const sessionRows = sessionIds.length > 0
    ? await runQuery(
        supabase
          .from('review_sessions')
          .select('id, analysis_mode, file_name, columns, total_processed, actionable_records, approved_count, rejected_count, pending_count, event_count, history_warning, uploaded_at, updated_at')
          .in('id', sessionIds),
        'Failed to load ledger sessions.',
      )
    : [];
  const sessionMap = new Map((sessionRows ?? []).map(row => [(row as ReviewSessionRow).id, row as ReviewSessionRow]));
  const approvedRecords = allRecords.filter(row => row.decision_status === 'approved');

  return {
    window_days: windowDays,
    generated_at: new Date().toISOString(),
    session_count: new Set(allRecords.map(row => row.session_id)).size,
    record_count: allRecords.length,
    approved_count: approvedRecords.length,
    rejected_count: allRecords.filter(row => row.decision_status === 'declined').length,
    pending_count: allRecords.filter(row => row.decision_status === 'pending' || row.decision_status === 'manual_review').length,
    approved_amount: approvedRecords.reduce((sum, row) => sum + (row.amount ?? 0), 0),
    average_amount: approvedRecords.length > 0 ? approvedRecords.reduce((sum, row) => sum + (row.amount ?? 0), 0) / approvedRecords.length : 0,
    flagged_approved_count: approvedRecords.filter(row => row.category === 'anomaly').length,
    clear_approved_count: approvedRecords.filter(row => row.category === 'clear').length,
    records: approvedRecords.map(row => {
      const session = sessionMap.get(row.session_id);
      return {
        ...mapReviewRecordRow(row),
        session_id: row.session_id,
        file_name: session?.file_name ?? null,
        analysis_mode: session?.analysis_mode ?? null,
        uploaded_at: session?.uploaded_at ?? null,
        updated_at: row.updated_at,
        latest_activity_at: row.updated_at,
        approved_at: row.updated_at,
      };
    }),
  };
}
