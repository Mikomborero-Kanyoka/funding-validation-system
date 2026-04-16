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
  CURRENCY?: string | null;
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
  currency?: string | null;
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
const CSV_RESPONSE_REPORT_RECIPIENT = 'aiqkanyoka@gmail.com';

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

function normalizeDisplayText(value: unknown) {
  const normalized = normalizeScalar(value);
  if (normalized === null || normalized instanceof Date) return null;
  const text = String(normalized).trim();
  return text || null;
}

function normalizeCurrency(value: unknown) {
  const currency = normalizeDisplayText(value);
  return currency ? currency.toUpperCase() : null;
}

function extractCurrency(row: SourceRow | Record<string, unknown> | null | undefined) {
  if (!row) return null;
  return normalizeCurrency(
    getFirstPresent(row as SourceRow, 'CURRENCY', 'CCY', 'CUR', 'CURRENCY CODE', 'CURRENCY_CODE'),
  );
}

function uniqueDisplayNames(values: unknown[]) {
  const seen = new Set<string>();
  const result: string[] = [];

  values.forEach(value => {
    const normalized = normalizeName(value);
    const display = normalizeDisplayText(value);
    if (!normalized || !display || seen.has(normalized)) return;
    seen.add(normalized);
    result.push(display);
  });

  return result;
}

function joinNames(names: string[]) {
  if (names.length === 0) return 'multiple people';
  if (names.length === 1) return names[0];
  if (names.length === 2) return `${names[0]} and ${names[1]}`;
  if (names.length === 3) return `${names[0]}, ${names[1]}, and ${names[2]}`;
  return `${names[0]}, ${names[1]}, ${names[2]}, and ${names.length - 3} others`;
}

function buildPossibleFraudReason(identifierLabel: 'EC number' | 'Account number', names: string[]) {
  return `${identifierLabel} used by ${joinNames(names)} possible fraud`;
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
    currency: extractCurrency(row),
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
    CURRENCY: (base.CURRENCY ?? extractCurrency((row.row_data ?? {}) as SourceRow) ?? null) as string | null,
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

  if (recentMatches.length > 0) anomalyReasons.push(`Previous loan found within the last ${RECENT_APPLICATION_WINDOW_DAYS} days`);

  if (matchedRows.length > 0) {
    const currentApplicantName = normalizeDisplayText(application.applicant_name);
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
      const allNames = uniqueDisplayNames([
        ...group.map(item => item.customerName1),
        ...(appEcKey === ecKey && currentApplicantName ? [currentApplicantName] : []),
      ]);
      if (allNames.length > 1) {
        anomalyReasons.push(buildPossibleFraudReason('EC number', allNames));
        break;
      }
    }

    if (!anomalyReasons.some(reason => reason.endsWith('possible fraud'))) {
      const customerGroups = new Map<string, PreparedHistoryRow[]>();
      matchedRows.forEach(historyRow => {
        if (!historyRow.customerKey) return;
        const existing = customerGroups.get(historyRow.customerKey) ?? [];
        existing.push(historyRow);
        customerGroups.set(historyRow.customerKey, existing);
      });

      for (const [customerKey, group] of customerGroups.entries()) {
        const allNames = uniqueDisplayNames([
          ...group.map(item => item.customerName1),
          ...(appCustomerKey === customerKey && currentApplicantName ? [currentApplicantName] : []),
        ]);
        if (allNames.length > 1) {
          anomalyReasons.push(buildPossibleFraudReason('Account number', allNames));
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
    currency: extractCurrency(row),
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
    currency: extractCurrency(row.source_row),
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

async function loadSessionSourceRows(sessionId: string) {
  let rowsById = sessionSourceRowCache.get(sessionId);
  if (rowsById) return rowsById;

  const recordRows = await runQuery(
    supabase.from('review_records').select('application_id, source_row').eq('session_id', sessionId),
    'Failed to load downloadable rows for the current session.',
  );
  rowsById = Object.fromEntries(
    (recordRows ?? []).map(row => [
      (row as { application_id: string }).application_id,
      ((row as { source_row: SourceRow | null }).source_row ?? {}) as SourceRow,
    ]),
  );
  sessionSourceRowCache.set(sessionId, rowsById);
  return rowsById;
}

function formatReportDateTime(value: string | Date) {
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsed);
}

function formatReportFieldLabel(label: string) {
  return label
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, character => character.toUpperCase());
}

function formatReportValue(value: JsonValue | undefined): string {
  if (value === null || value === undefined || value === '') return 'N/A';
  if (Array.isArray(value)) {
    const parts = value.map(item => formatReportValue(item)).filter(item => item !== 'N/A');
    return parts.length > 0 ? parts.join(', ') : 'N/A';
  }
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function formatReportCurrency(value: number | null | undefined, currency?: string | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  const amount = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
  const code = normalizeCurrency(currency);
  return code ? `${code} ${amount}` : amount;
}

function formatDecisionStatus(status: DecisionStatus) {
  if (status === 'manual_review') return 'Manual review';
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function getCsvRecordName(record: ReviewRecord, sourceRow: SourceRow) {
  return normalizeDisplayText(record.applicant_name)
    ?? normalizeDisplayText(getFirstPresent(sourceRow, 'BeneficiaryName', 'BENEFICIARY NAME', 'NAME', 'APPLICANT NAME'))
    ?? `Row ${record.row}`;
}

function getCsvRecordReference(record: ReviewRecord, sourceRow: SourceRow) {
  return normalizeDisplayText(record.ec_number)
    ?? normalizeDisplayText(record.customer_no)
    ?? normalizeDisplayText(getFirstPresent(sourceRow, 'Reference', 'REFERENCE', 'REF', 'ID'))
    ?? 'N/A';
}

function getCsvRecordResponse(record: ReviewRecord, sourceRow: SourceRow) {
  return normalizeDisplayText(record.response_status)
    ?? normalizeDisplayText(getFirstPresent(sourceRow, 'BeneficiaryStatus', 'Status', 'STATUS', 'DECISION', 'RESULT', 'APPLICATION_STATUS'))
    ?? record.reason;
}

function escapePdfText(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[^\x20-\x7E]/g, '?')
    .replace(/\\/g, '\\\\')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)');
}

type PdfColor = [number, number, number];

type CsvResponseReportField = {
  label: string;
  value: string;
};

type CsvResponseRejectedRecord = {
  title: string;
  reference: string;
  amount: string;
  response: string;
  reviewStatus: string;
  fields: CsvResponseReportField[];
};

type CsvResponseReportData = {
  title: string;
  subtitle: string;
  sourceFileName: string;
  generatedAtLabel: string;
  acceptedCount: number;
  rejectedCount: number;
  rejectedRecords: CsvResponseRejectedRecord[];
};

function wrapPdfText(text: string, maxChars: number) {
  if (!text) return [''];
  const words = text.split(/\s+/).filter(Boolean);
  if (words.length === 0) return [''];

  const wrapped: string[] = [];
  let current = words.shift() ?? '';

  words.forEach(word => {
    const next = `${current} ${word}`;
    if (next.length <= maxChars) {
      current = next;
      return;
    }
    wrapped.push(current);
    current = word;
  });

  wrapped.push(current);
  return wrapped;
}

function pdfColor([red, green, blue]: PdfColor) {
  return `${(red / 255).toFixed(3)} ${(green / 255).toFixed(3)} ${(blue / 255).toFixed(3)}`;
}

function buildStyledCsvReportPdf(report: CsvResponseReportData) {
  const pageWidth = 612;
  const pageHeight = 792;
  const marginX = 36;
  const topMargin = 28;
  const bottomMargin = 28;
  const headerHeight = 78;
  const firstPageContentTop = pageHeight - topMargin - headerHeight - 20;
  const continuedPageTop = pageHeight - topMargin - 30;
  const contentWidth = pageWidth - marginX * 2;

  const colors = {
    header: [15, 23, 42] as PdfColor,
    headerAccent: [14, 165, 233] as PdfColor,
    text: [30, 41, 59] as PdfColor,
    muted: [100, 116, 139] as PdfColor,
    light: [241, 245, 249] as PdfColor,
    border: [203, 213, 225] as PdfColor,
    card: [248, 250, 252] as PdfColor,
    accepted: [16, 185, 129] as PdfColor,
    rejected: [244, 63, 94] as PdfColor,
    section: [217, 119, 6] as PdfColor,
    white: [255, 255, 255] as PdfColor,
  };

  type PdfPage = { commands: string[]; cursorTop: number; number: number };
  const pages: PdfPage[] = [];

  function startPage(firstPage: boolean) {
    const pageNumber = pages.length + 1;
    const page: PdfPage = { commands: [], cursorTop: firstPage ? firstPageContentTop : continuedPageTop, number: pageNumber };

    if (firstPage) {
      const headerTop = pageHeight - topMargin;
      const headerBottom = headerTop - headerHeight;
      page.commands.push(`${pdfColor(colors.header)} rg`);
      page.commands.push(`${marginX} ${headerBottom} ${contentWidth} ${headerHeight} re f`);
      page.commands.push(`${pdfColor(colors.headerAccent)} rg`);
      page.commands.push(`${marginX} ${headerBottom} ${contentWidth} 8 re f`);

      page.commands.push(
        'BT',
        `/F2 24 Tf`,
        `${pdfColor(colors.white)} rg`,
        `1 0 0 1 ${marginX + 20} ${headerTop - 34} Tm`,
        `(${escapePdfText(report.title)}) Tj`,
        'ET',
      );
      page.commands.push(
        'BT',
        `/F1 10 Tf`,
        `${pdfColor(colors.white)} rg`,
        `1 0 0 1 ${marginX + 20} ${headerTop - 52} Tm`,
        `(${escapePdfText(report.subtitle)}) Tj`,
        'ET',
      );
    } else {
      page.commands.push(
        'BT',
        `/F2 13 Tf`,
        `${pdfColor(colors.text)} rg`,
        `1 0 0 1 ${marginX} ${pageHeight - topMargin - 6} Tm`,
        `(${escapePdfText(`${report.title} - Continued`)}) Tj`,
        'ET',
      );
      page.commands.push(`${pdfColor(colors.border)} RG`);
      page.commands.push(`${marginX} ${pageHeight - topMargin - 16} m ${pageWidth - marginX} ${pageHeight - topMargin - 16} l S`);
    }

    pages.push(page);
    return page;
  }

  let page = startPage(true);

  function ensureSpace(height: number) {
    if (page.cursorTop - height < bottomMargin) {
      page = startPage(false);
    }
  }

  function drawTextBlock(
    x: number,
    top: number,
    text: string,
    options: { font: 'F1' | 'F2'; size: number; color: PdfColor; maxChars?: number; lineHeight?: number },
  ) {
    const lineHeight = options.lineHeight ?? Math.max(12, options.size + 2);
    const lines = wrapPdfText(text, options.maxChars ?? 60);
    lines.forEach((line, index) => {
      page.commands.push(
        'BT',
        `/${options.font} ${options.size} Tf`,
        `${pdfColor(options.color)} rg`,
        `1 0 0 1 ${x} ${top - options.size - index * lineHeight} Tm`,
        `(${escapePdfText(line)}) Tj`,
        'ET',
      );
    });
    return lines.length * lineHeight;
  }

  function drawInfoCard(x: number, width: number, label: string, value: string, height: number) {
    const top = page.cursorTop;
    page.commands.push(`${pdfColor(colors.light)} rg`);
    page.commands.push(`${pdfColor(colors.border)} RG`);
    page.commands.push(`${x} ${top - height} ${width} ${height} re B`);
    drawTextBlock(x + 12, top - 12, label, { font: 'F2', size: 9, color: colors.muted, maxChars: 20, lineHeight: 11 });
    drawTextBlock(x + 12, top - 28, value, { font: 'F1', size: 10, color: colors.text, maxChars: Math.max(24, Math.floor((width - 24) / 5.4)), lineHeight: 12 });
  }

  function drawStatCard(x: number, width: number, label: string, value: string, accent: PdfColor, height: number) {
    const top = page.cursorTop;
    page.commands.push(`${pdfColor(colors.card)} rg`);
    page.commands.push(`${pdfColor(colors.border)} RG`);
    page.commands.push(`${x} ${top - height} ${width} ${height} re B`);
    page.commands.push(`${pdfColor(accent)} rg`);
    page.commands.push(`${x} ${top - height} ${width} 6 re f`);
    drawTextBlock(x + 12, top - 16, label, { font: 'F2', size: 9, color: colors.muted, maxChars: 18, lineHeight: 11 });
    drawTextBlock(x + 12, top - 34, value, { font: 'F2', size: 18, color: colors.text, maxChars: 12, lineHeight: 18 });
  }

  function drawSectionTitle(title: string) {
    const height = 28;
    ensureSpace(height + 10);
    const top = page.cursorTop;
    page.commands.push(`${pdfColor(colors.section)} rg`);
    page.commands.push(`${marginX} ${top - height} ${contentWidth} ${height} re f`);
    drawTextBlock(marginX + 14, top - 7, title, { font: 'F2', size: 12, color: colors.white, maxChars: 50, lineHeight: 13 });
    page.cursorTop -= height + 14;
  }

  function estimateRecordHeight(record: CsvResponseRejectedRecord) {
    const topLines =
      wrapPdfText(record.title, 60).length +
      wrapPdfText(`Reference: ${record.reference}`, 70).length +
      wrapPdfText(`Amount: ${record.amount}`, 70).length +
      wrapPdfText(`Response: ${record.response}`, 70).length +
      wrapPdfText(`Review status: ${record.reviewStatus}`, 70).length;
    const fieldLines = record.fields.reduce((count, field) => count + wrapPdfText(`${field.label}: ${field.value}`, 72).length, 0);
    return 34 + topLines * 13 + (record.fields.length > 0 ? 18 + fieldLines * 11 : 0) + 18;
  }

  function drawRejectedRecord(record: CsvResponseRejectedRecord, index: number) {
    const height = estimateRecordHeight(record);
    ensureSpace(height);
    const top = page.cursorTop;

    page.commands.push(`${pdfColor(colors.card)} rg`);
    page.commands.push(`${pdfColor(colors.border)} RG`);
    page.commands.push(`${marginX} ${top - height} ${contentWidth} ${height} re B`);
    page.commands.push(`${pdfColor(colors.rejected)} rg`);
    page.commands.push(`${marginX} ${top - 26} ${contentWidth} 26 re f`);

    drawTextBlock(marginX + 14, top - 7, `${index + 1}. ${record.title}`, {
      font: 'F2',
      size: 12,
      color: colors.white,
      maxChars: 62,
      lineHeight: 13,
    });

    let contentTop = top - 40;
    const leftX = marginX + 14;
    contentTop -= drawTextBlock(leftX, contentTop, `Reference: ${record.reference}`, {
      font: 'F1',
      size: 10,
      color: colors.text,
      maxChars: 72,
      lineHeight: 12,
    }) + 2;
    contentTop -= drawTextBlock(leftX, contentTop, `Amount: ${record.amount}`, {
      font: 'F1',
      size: 10,
      color: colors.text,
      maxChars: 72,
      lineHeight: 12,
    }) + 2;
    contentTop -= drawTextBlock(leftX, contentTop, `Response: ${record.response}`, {
      font: 'F1',
      size: 10,
      color: colors.text,
      maxChars: 72,
      lineHeight: 12,
    }) + 2;
    contentTop -= drawTextBlock(leftX, contentTop, `Review status: ${record.reviewStatus}`, {
      font: 'F1',
      size: 10,
      color: colors.text,
      maxChars: 72,
      lineHeight: 12,
    }) + 4;

    if (record.fields.length > 0) {
      contentTop -= drawTextBlock(leftX, contentTop, 'Uploaded fields', {
        font: 'F2',
        size: 10,
        color: colors.muted,
        maxChars: 30,
        lineHeight: 11,
      });

      record.fields.forEach(field => {
        contentTop -= drawTextBlock(leftX + 8, contentTop, `${field.label}: ${field.value}`, {
          font: 'F1',
          size: 9,
          color: colors.text,
          maxChars: 74,
          lineHeight: 11,
        }) + 1;
      });
    }

    page.cursorTop -= height + 12;
  }

  ensureSpace(116);
  drawInfoCard(marginX, contentWidth, 'Source file', report.sourceFileName, 54);
  page.cursorTop -= 66;
  drawInfoCard(marginX, contentWidth * 0.62 - 6, 'Generated', report.generatedAtLabel, 48);
  drawStatCard(marginX + contentWidth * 0.62 + 6, contentWidth * 0.18 - 6, 'Accepted', String(report.acceptedCount), colors.accepted, 48);
  drawStatCard(marginX + contentWidth * 0.80 + 12, contentWidth * 0.20 - 12, 'Rejected', String(report.rejectedCount), colors.rejected, 48);
  page.cursorTop -= 62;

  drawSectionTitle('Rejected Records');

  if (report.rejectedRecords.length === 0) {
    ensureSpace(60);
    const top = page.cursorTop;
    page.commands.push(`${pdfColor(colors.light)} rg`);
    page.commands.push(`${pdfColor(colors.border)} RG`);
    page.commands.push(`${marginX} ${top - 52} ${contentWidth} 52 re B`);
    drawTextBlock(marginX + 14, top - 15, 'No rejected records were found in this CSV response batch.', {
      font: 'F1',
      size: 11,
      color: colors.text,
      maxChars: 72,
      lineHeight: 13,
    });
    page.cursorTop -= 64;
  } else {
    report.rejectedRecords.forEach((record, index) => drawRejectedRecord(record, index));
  }

  pages.forEach(pdfPage => {
    pdfPage.commands.push(
      'BT',
      `/F1 9 Tf`,
      `${pdfColor(colors.muted)} rg`,
      `1 0 0 1 ${pageWidth - marginX - 70} 18 Tm`,
      `(${escapePdfText(`Page ${pdfPage.number}`)}) Tj`,
      'ET',
    );
  });

  const pageCount = pages.length;
  const regularFontObjectId = 3 + pageCount * 2;
  const boldFontObjectId = regularFontObjectId + 1;
  const objects: string[] = new Array(boldFontObjectId);
  const encoder = new TextEncoder();
  const pageRefs: string[] = [];

  objects[0] = '<< /Type /Catalog /Pages 2 0 R >>';

  pages.forEach((pdfPage, pageIndex) => {
    const pageObjectId = 3 + pageIndex * 2;
    const contentObjectId = pageObjectId + 1;
    const content = pdfPage.commands.join('\n');
    pageRefs.push(`${pageObjectId} 0 R`);
    objects[pageObjectId - 1] = `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 ${regularFontObjectId} 0 R /F2 ${boldFontObjectId} 0 R >> >> /Contents ${contentObjectId} 0 R >>`;
    objects[contentObjectId - 1] = `<< /Length ${encoder.encode(content).length} >>\nstream\n${content}\nendstream`;
  });

  objects[1] = `<< /Type /Pages /Kids [${pageRefs.join(' ')}] /Count ${pageCount} >>`;
  objects[regularFontObjectId - 1] = '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>';
  objects[boldFontObjectId - 1] = '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>';

  let pdf = '%PDF-1.4\n';
  const offsets = [0];

  objects.forEach((object, index) => {
    offsets[index + 1] = pdf.length;
    pdf += `${index + 1} 0 obj\n${object}\nendobj\n`;
  });

  const xrefOffset = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (let objectId = 1; objectId <= objects.length; objectId += 1) {
    pdf += `${String(offsets[objectId]).padStart(10, '0')} 00000 n \n`;
  }
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF`;

  return new Blob([pdf], { type: 'application/pdf' });
}

async function buildCsvResponseReport(results: AnalysisResponse) {
  if (results.analysis_mode !== 'csv') fail('PDF reports are only available for CSV response uploads.');

  const rowsById = await loadSessionSourceRows(results.session_id);
  const records = [...results.anomalies, ...results.clear_records].sort((left, right) => left.row - right.row);
  const acceptedRecords = records.filter(record => record.decision_status === 'approved');
  const rejectedRecords = records.filter(record => record.decision_status === 'declined');
  const generatedAt = new Date();
  const rejectedDetails: CsvResponseRejectedRecord[] = rejectedRecords.map(record => {
    const sourceRow = rowsById[record.application_id] ?? {};
    const fields = Object.entries(sourceRow)
      .map(([key, value]) => ({
        label: formatReportFieldLabel(key),
        value: formatReportValue(value as JsonValue),
      }))
      .filter(field => field.value !== 'N/A');

    return {
      title: getCsvRecordName(record, sourceRow),
      reference: getCsvRecordReference(record, sourceRow),
      amount: formatReportCurrency(record.amount ?? null, record.currency ?? extractCurrency(sourceRow)),
      response: getCsvRecordResponse(record, sourceRow),
      reviewStatus: formatDecisionStatus(record.decision_status),
      fields,
    };
  });

  const pdfReport: CsvResponseReportData = {
    title: 'CSV Response Report',
    subtitle: 'Accepted total with detailed rejected records',
    sourceFileName: results.file_name,
    generatedAtLabel: formatReportDateTime(generatedAt),
    acceptedCount: acceptedRecords.length,
    rejectedCount: rejectedDetails.length,
    rejectedRecords: rejectedDetails,
  };

  return {
    blob: buildStyledCsvReportPdf(pdfReport),
    fileName: `${results.file_name.replace(/\.[^.]+$/i, '')}_responses_report.pdf`,
    recipient: CSV_RESPONSE_REPORT_RECIPIENT,
    acceptedCount: acceptedRecords.length,
    rejectedCount: rejectedDetails.length,
    generatedAt: generatedAt.toISOString(),
  };
}

async function blobToBase64(blob: Blob) {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  let binary = '';
  const chunkSize = 0x8000;

  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }

  return btoa(binary);
}

export async function downloadCsvResponseReport(results: AnalysisResponse) {
  return buildCsvResponseReport(results);
}

export async function emailCsvResponseReport(results: AnalysisResponse, recipient = CSV_RESPONSE_REPORT_RECIPIENT) {
  const report = await buildCsvResponseReport(results);
  const pdfBase64 = await blobToBase64(report.blob);
  const { data, error } = await supabase.functions.invoke('send-csv-report', {
    body: {
      fileName: report.fileName,
      sourceFileName: results.file_name,
      acceptedCount: report.acceptedCount,
      rejectedCount: report.rejectedCount,
      generatedAt: report.generatedAt,
      pdfBase64,
    },
  });

  if (error) {
    const message = error.message || '';
    if (message.toLowerCase().includes('failed to send a request')) {
      fail('The email function is not reachable yet. Deploy the `send-csv-report` Supabase Edge Function and make sure `verify_jwt = false` is set in `supabase/config.toml`.');
    }
    fail(message || 'Failed to email the CSV response report.');
  }
  return {
    recipient,
    ...(data as { message?: string; recipient?: string }),
  };
}

export async function downloadSessionResults(results: AnalysisResponse) {
  const rowsById = await loadSessionSourceRows(results.session_id);
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
      .select('account_number, customer_name1, ec_number, customer_no, amount_financed, book_date, row_data')
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
    CURRENCY: extractCurrency(((row as HistoryRecordRow).row_data ?? {}) as SourceRow),
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
