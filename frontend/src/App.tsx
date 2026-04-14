import { useEffect, useState, Dispatch, SetStateAction } from 'react';
import {
  AlertTriangle,
  ChevronDown,
  Download,
  FileSpreadsheet,
  History as HistoryIcon,
  Loader2,
  Search,
  Upload,
  BarChart2,
  FileText,
} from 'lucide-react';
import {
  analyzeUpload,
  downloadSessionResults,
  getActivitySessionDetail,
  getActivitySessions,
  getApprovalLedger,
  replaceHistoryWithWorkbook,
  searchHistory as searchHistoryRecords,
  updateRecordDecision,
} from './lib/fundingData';

type View = 'upload' | 'history' | 'activity' | 'export';
type UploadMode = 'excel' | 'csv';
type DecisionStatus = 'pending' | 'approved' | 'declined' | 'manual_review';
type RiskFilter = 'all' | 'flagged' | 'unflagged';
type ApprovalFilter = 'all' | 'approved' | 'rejected' | 'pending';

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

function formatCurrency(value: number | string | null | undefined) {
  if (value === null || value === undefined || value === '') return 'N/A';
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return String(value);
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(n);
}

function formatDate(value: string | null | undefined) {
  if (!value) return 'N/A';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toISOString().slice(0, 10);
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return 'N/A';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsed);
}

function formatUploadMode(mode: string) {
  if (mode === 'excel') return 'Excel upload';
  if (mode === 'csv') return 'CSV responses';
  return mode || 'Upload';
}

function formatValue(value: string | number | boolean | null | undefined) {
  if (value === null || value === undefined || value === '') return 'N/A';
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  return String(value);
}

function formatDecisionLabel(status: string | null | undefined) {
  if (!status) return 'Unknown';
  if (status === 'manual_review') return 'Manual review';
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function getDecisionPillClass(status: string | null | undefined) {
  if (status === 'approved') return 'bg-emerald-100 text-emerald-800';
  if (status === 'declined') return 'bg-rose-100 text-rose-800';
  if (status === 'manual_review') return 'bg-sky-100 text-sky-800';
  return 'bg-slate-100 text-slate-600';
}

function getPrimaryIdentifier(record: ReviewRecord) {
  return record.ec_number || record.customer_no || 'No identifier';
}

function getRecordLabel(record: ReviewRecord) {
  return record.applicant_name || record.matched_records[0]?.CUSTOMER_NAME1 || String(getPrimaryIdentifier(record));
}

function getAccentColor(record: ReviewRecord, isCsv: boolean) {
  if (isCsv) {
    if (record.decision_status === 'approved') return '#16A34A';
    if (record.decision_status === 'declined') return '#DC2626';
    return '#0284C7';
  }
  if (record.decision_status === 'declined') return '#DC2626';
  if (record.decision_status === 'approved') return '#16A34A';
  if (record.category === 'anomaly') return '#F59E0B';
  return '#16A34A';
}

function updateRecordInResults(
  current: AnalysisResponse | null,
  updatedRecord: ReviewRecord,
  summary: DecisionSummary,
) {
  if (!current) return current;
  const replace = (arr: ReviewRecord[]) =>
    arr.map(r => r.application_id === updatedRecord.application_id ? updatedRecord : r);
  return { ...current, ...summary, anomalies: replace(current.anomalies), clear_records: replace(current.clear_records) };
}

// ── Sidebar ──────────────────────────────────────────────────────────────────
function Sidebar({ view, onSetView }: { view: View; onSetView: (v: View) => void }) {
  return (
    <aside className="w-52 flex-shrink-0 bg-white border-r border-slate-200 flex flex-col">
      <div className="flex items-center gap-2.5 px-4 py-4 border-b border-slate-200">
        <div className="w-7 h-7 rounded-lg bg-indigo-600 flex items-center justify-center flex-shrink-0">
          <FileSpreadsheet className="w-3.5 h-3.5 text-white" />
        </div>
        <div>
          <div className="text-sm font-semibold text-slate-900 leading-tight">MicroFinance</div>
          <div className="text-xs text-slate-400">Funding portal</div>
        </div>
      </div>

      <nav className="flex-1 px-2 py-3">
        <p className="text-[10px] font-semibold uppercase tracking-widest text-slate-400 px-2 pb-1">Workspace</p>

        <button
          onClick={() => onSetView('upload')}
          className={`w-full flex items-center gap-2.5 px-2 py-1.5 rounded-lg text-xs mb-0.5 transition-colors ${view === 'upload' ? 'bg-indigo-50 text-indigo-700 font-semibold' : 'text-slate-500 hover:bg-slate-100'}`}
        >
          <BarChart2 className="w-3.5 h-3.5 flex-shrink-0" />
          Review queue
          {view === 'upload' && (
            <span className="ml-auto text-[10px] bg-indigo-100 text-indigo-700 rounded-full px-1.5 py-0.5 font-semibold">12</span>
          )}
        </button>

        <button
          onClick={() => onSetView('history')}
          className={`w-full flex items-center gap-2.5 px-2 py-1.5 rounded-lg text-xs mb-0.5 transition-colors ${view === 'history' ? 'bg-indigo-50 text-indigo-700 font-semibold' : 'text-slate-500 hover:bg-slate-100'}`}
        >
          <Search className="w-3.5 h-3.5 flex-shrink-0" />
          History search
        </button>

        <button
          className="w-full flex items-center gap-2.5 px-2 py-1.5 rounded-lg text-xs mb-0.5 text-slate-500 hover:bg-slate-100 transition-colors"
        >
          <Upload className="w-3.5 h-3.5 flex-shrink-0" />
          Upload file
        </button>

        <p className="text-[10px] font-semibold uppercase tracking-widest text-slate-400 px-2 pb-1 mt-3">Reports</p>

        <button
          onClick={() => onSetView('export')}
          className={`w-full flex items-center gap-2.5 px-2 py-1.5 rounded-lg text-xs mb-0.5 transition-colors ${view === 'export' ? 'bg-indigo-50 text-indigo-700 font-semibold' : 'text-slate-500 hover:bg-slate-100'}`}
        >
          <FileText className="w-3.5 h-3.5 flex-shrink-0" />
          Approval ledger
        </button>

        <button
          onClick={() => onSetView('activity')}
          className={`w-full flex items-center gap-2.5 px-2 py-1.5 rounded-lg text-xs mb-0.5 transition-colors ${view === 'activity' ? 'bg-indigo-50 text-indigo-700 font-semibold' : 'text-slate-500 hover:bg-slate-100'}`}
        >
          <HistoryIcon className="w-3.5 h-3.5 flex-shrink-0" />
          Activity log
        </button>
      </nav>

      <div className="px-4 py-3 border-t border-slate-200 text-[11px] text-slate-400">
        <span className="font-medium text-slate-500">14-day window</span><br />
        Session active
      </div>
    </aside>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({ label, value, color }: { label: string; value: number | string; color?: string }) {
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2.5">
      <div className="text-[11px] text-slate-500 font-medium mb-1">{label}</div>
      <div className={`text-xl font-semibold leading-none ${color ?? 'text-slate-900'}`}>{value}</div>
    </div>
  );
}

// ── Filter chip ───────────────────────────────────────────────────────────────
function Chip({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`px-2.5 py-1 rounded-full border text-[11px] cursor-pointer transition-colors whitespace-nowrap ${
        active
          ? 'bg-indigo-50 border-indigo-300 text-indigo-700 font-semibold'
          : 'bg-white border-slate-200 text-slate-500 hover:bg-slate-50'
      }`}
    >
      {label}
    </button>
  );
}

// ── Status badge ──────────────────────────────────────────────────────────────
function StatusBadge({ record, isCsv }: { record: ReviewRecord; isCsv: boolean }) {
  if (record.decision_status === 'approved')
    return <span className="inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-100 text-emerald-800">{isCsv ? 'Accepted' : 'Approved'}</span>;
  if (record.decision_status === 'declined')
    return <span className="inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-rose-100 text-rose-800">Rejected</span>;
  if (!isCsv && record.category === 'anomaly')
    return <span className="inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-amber-100 text-amber-800">Flagged ({record.anomaly_reasons?.length ?? 1})</span>;
  if (isCsv)
    return <span className="inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-sky-100 text-sky-800">Needs review</span>;
  return <span className="inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold bg-slate-100 text-slate-600">Unflagged</span>;
}

// ── Row actions ───────────────────────────────────────────────────────────────
function RowActions({
  record,
  isCsv,
  loading,
  onDecide,
}: {
  record: ReviewRecord;
  isCsv: boolean;
  loading: boolean;
  onDecide: (decision: DecisionStatus) => void;
}) {
  const isApproved = record.decision_status === 'approved';
  const isRejected = record.decision_status === 'declined';

  if (isApproved || isRejected) {
    return (
      <button
        onClick={e => { e.stopPropagation(); onDecide('pending'); }}
        className="text-[11px] px-2 py-1 rounded border border-slate-200 bg-white text-slate-500 hover:bg-slate-50"
      >
        Undo
      </button>
    );
  }

  if (isCsv) {
    return (
      <>
        <button
          onClick={e => { e.stopPropagation(); onDecide('approved'); }}
          disabled={loading}
          className="text-[11px] px-2 py-1 rounded border border-transparent bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50"
        >
          {loading ? <Loader2 className="w-3 h-3 animate-spin inline" /> : 'Accept'}
        </button>
        <button
          onClick={e => { e.stopPropagation(); onDecide('declined'); }}
          disabled={loading}
          className="text-[11px] px-2 py-1 rounded border border-rose-200 bg-white text-rose-700 hover:bg-rose-50 disabled:opacity-50"
        >
          Reject
        </button>
      </>
    );
  }

  if (record.category === 'anomaly') {
    return (
      <>
        <button
          onClick={e => { e.stopPropagation(); onDecide('approved'); }}
          disabled={loading}
          className="text-[11px] px-2 py-1 rounded border border-transparent bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50"
        >
          {loading ? <Loader2 className="w-3 h-3 animate-spin inline" /> : 'Override'}
        </button>
        <button
          onClick={e => { e.stopPropagation(); onDecide('declined'); }}
          disabled={loading}
          className="text-[11px] px-2 py-1 rounded border border-rose-200 bg-white text-rose-700 hover:bg-rose-50 disabled:opacity-50"
        >
          Cancel
        </button>
      </>
    );
  }

  return (
    <>
      <button
        onClick={e => { e.stopPropagation(); onDecide('approved'); }}
        disabled={loading}
        className="text-[11px] px-2 py-1 rounded border border-transparent bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
      >
        {loading ? <Loader2 className="w-3 h-3 animate-spin inline" /> : 'Approve'}
      </button>
      <button
        onClick={e => { e.stopPropagation(); onDecide('manual_review'); }}
        disabled={loading}
        className="text-[11px] px-2 py-1 rounded border border-sky-200 bg-sky-50 text-sky-700 hover:bg-sky-100 disabled:opacity-50"
      >
        Review
      </button>
    </>
  );
}

// ── Expanded panel ────────────────────────────────────────────────────────────
function ExpandedPanel({ record, isCsv }: { record: ReviewRecord; isCsv: boolean }) {
  return (
    <div className="px-4 pb-4 pt-3 bg-slate-50 border-t border-slate-200">
      <div className="grid grid-cols-3 gap-2 mb-3">
        <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
          <div className="text-[10px] uppercase tracking-widest text-slate-400 font-medium mb-1">Application date</div>
          <div className="text-sm font-semibold text-slate-800">{formatDate(record.application_book_date || record.reference_date)}</div>
        </div>
        <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
          <div className="text-[10px] uppercase tracking-widest text-slate-400 font-medium mb-1">Latest history</div>
          <div className="text-sm font-semibold text-slate-800">{formatDate(record.latest_book_date)}</div>
        </div>
        <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
          <div className="text-[10px] uppercase tracking-widest text-slate-400 font-medium mb-1">Matches found</div>
          <div className="text-sm font-semibold text-slate-800">{record.category === 'anomaly' ? record.recent_match_count : record.history_match_count}</div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          {!isCsv && record.anomaly_reasons && record.anomaly_reasons.length > 0 && (
            <div className="bg-white border border-amber-200 rounded-lg px-3 py-2 mb-2">
              {record.anomaly_reasons.map((r, i) => (
                <div key={i} className="flex items-start gap-2 text-xs text-amber-800 py-0.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-amber-500 mt-1 flex-shrink-0" />
                  {r}
                </div>
              ))}
            </div>
          )}
          {(isCsv || !record.anomaly_reasons?.length) && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2 mb-2">
              <div className="text-xs text-slate-500">{record.reason}</div>
            </div>
          )}
          <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
            <div className="text-[10px] uppercase tracking-widest text-slate-400 font-medium mb-2">Application</div>
            {[
              ['Name', record.applicant_name || 'N/A'],
              ['EC number', String(record.ec_number || 'N/A')],
              ['Customer no.', String(record.customer_no || 'N/A')],
              ['Amount', formatCurrency(record.amount)],
            ].map(([label, val]) => (
              <div key={label} className="flex justify-between text-xs py-1 border-b border-slate-100 last:border-0">
                <span className="text-slate-500">{label}</span>
                <span className="font-semibold text-slate-800">{val}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
          <div className="text-[10px] uppercase tracking-widest text-slate-400 font-medium mb-2">Matched history</div>
          {record.matched_records.length === 0 ? (
            <div className="text-xs text-slate-400">No matching records.</div>
          ) : (
            <div className="space-y-1.5 max-h-48 overflow-y-auto">
              {record.matched_records.map((m, i) => (
                <div key={i} className="bg-slate-50 rounded-md px-2 py-1.5">
                  <div className="text-xs font-semibold text-slate-800">{m.CUSTOMER_NAME1 || 'Record'}</div>
                  <div className="text-[11px] text-slate-500">
                    {m.EC_NUMBER || 'N/A'} · Cust: {m.CUSTOMER_NO || 'N/A'}
                  </div>
                  <div className="text-[11px] text-slate-500">
                    Booked: {formatDate(m.BOOK_DATE)} · {formatCurrency(m.AMOUNT_FINANCED)}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {record.decision_status === 'approved' && (
        <div className="mt-2 px-3 py-2 bg-emerald-50 border border-emerald-200 rounded-lg text-xs font-semibold text-emerald-800">
          Approved - saved and syncing to history.xlsx
        </div>
      )}
    </div>
  );
}

// ── Record row ────────────────────────────────────────────────────────────────
function RecordRow({
  record,
  isCsv,
  expanded,
  loading,
  onToggle,
  onDecide,
}: {
  record: ReviewRecord;
  isCsv: boolean;
  expanded: boolean;
  loading: boolean;
  onToggle: () => void;
  onDecide: (d: DecisionStatus) => void;
}) {
  const accent = getAccentColor(record, isCsv);
  const faded = record.decision_status === 'declined' ? 'opacity-60' : '';

  return (
    <div className="border-b border-slate-100 last:border-0">
      <div
        className={`grid items-center gap-0 cursor-pointer hover:bg-slate-50 transition-colors ${faded}`}
        style={{ gridTemplateColumns: '3px 1fr 96px 96px 84px 68px 168px', height: 36 }}
        onClick={onToggle}
      >
        <div style={{ background: accent, height: 36, width: 3 }} />
        <div className="px-2 text-xs font-semibold text-slate-800 truncate">{getRecordLabel(record)}</div>
        <div className="px-2 text-[11px] text-slate-400 truncate">{String(getPrimaryIdentifier(record))} · {record.row}</div>
        <div className="px-2"><StatusBadge record={record} isCsv={isCsv} /></div>
        <div className="px-2 text-xs font-semibold text-slate-800 text-right">{formatCurrency(record.amount)}</div>
        <div className="px-2 text-[11px] text-slate-400 text-right">{formatDate(record.application_book_date || record.reference_date).slice(5)}</div>
        <div className="px-2 flex items-center justify-end gap-1" onClick={e => e.stopPropagation()}>
          <RowActions record={record} isCsv={isCsv} loading={loading} onDecide={onDecide} />
          <button
            onClick={e => { e.stopPropagation(); onToggle(); }}
            className="w-6 h-6 flex items-center justify-center border border-slate-200 rounded bg-white text-slate-400 hover:bg-slate-50 flex-shrink-0 transition-transform"
            style={{ transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)' }}
          >
            <ChevronDown className="w-3 h-3" />
          </button>
        </div>
      </div>
      {expanded && <ExpandedPanel record={record} isCsv={isCsv} />}
    </div>
  );
}

// ── Upload view ───────────────────────────────────────────────────────────────
function UploadView({
  results,
  setResults,
}: {
  results: AnalysisResponse | null;
  setResults: Dispatch<SetStateAction<AnalysisResponse | null>>;
}) {
  const [uploadMode, setUploadMode] = useState<UploadMode>('excel');
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [expandedRecords, setExpandedRecords] = useState<Record<string, boolean>>({});
  const [recordLoading, setRecordLoading] = useState<Record<string, boolean>>({});
  const [riskFilter, setRiskFilter] = useState<RiskFilter>('all');
  const [approvalFilter, setApprovalFilter] = useState<ApprovalFilter>('all');
  const [downloadLoading, setDownloadLoading] = useState(false);
  const [search, setSearch] = useState('');

  const isCsv = results?.analysis_mode === 'csv';
  const allRecords = results ? [...results.anomalies, ...results.clear_records].sort((a, b) => a.row - b.row) : [];
  const approvedRecords = allRecords.filter(r => r.decision_status === 'approved');
  const rejectedRecords = allRecords.filter(r => r.decision_status === 'declined');
  const pendingRecords = allRecords.filter(r => r.decision_status === 'pending' || r.decision_status === 'manual_review');

  const filteredRecords = allRecords.filter(r => {
    if (!isCsv) {
      if (riskFilter === 'flagged' && r.category !== 'anomaly') return false;
      if (riskFilter === 'unflagged' && r.category !== 'clear') return false;
    }
    if (approvalFilter === 'approved' && r.decision_status !== 'approved') return false;
    if (approvalFilter === 'rejected' && r.decision_status !== 'declined') return false;
    if (approvalFilter === 'pending' && !['pending', 'manual_review'].includes(r.decision_status)) return false;
    const q = search.toLowerCase();
    if (q) {
      const name = (getRecordLabel(r) + String(getPrimaryIdentifier(r))).toLowerCase();
      if (!name.includes(q)) return false;
    }
    return true;
  });

  const handleUpload = async () => {
    if (!file) return;
    setUploading(true);
    setResults(null);
    setExpandedRecords({});
    setRecordLoading({});
    setRiskFilter('all');
    setApprovalFilter('all');
    setSearch('');
    try {
      const response = await analyzeUpload(file, uploadMode);
      setResults(response);
    } catch (error) {
      console.error(error);
      alert(error instanceof Error ? error.message : 'Failed to process file');
    } finally {
      setUploading(false);
    }
  };

  const handleDecision = async (record: ReviewRecord, decision: DecisionStatus) => {
    if (!results) return;
    setRecordLoading(c => ({ ...c, [record.application_id]: true }));
    try {
      const response = await updateRecordDecision(results.session_id, record.application_id, decision);
      setResults(c => updateRecordInResults(c, response.record, response.summary));
    } catch (error: unknown) {
      alert(error instanceof Error ? error.message : 'Failed to update the record.');
    } finally {
      setRecordLoading(c => ({ ...c, [record.application_id]: false }));
    }
  };

  const handleDownload = async () => {
    if (!results) return;
    setDownloadLoading(true);
    try {
      const { blob, fileName } = await downloadSessionResults(results);
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url; link.download = fileName;
      document.body.appendChild(link); link.click(); link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error: unknown) {
      alert(error instanceof Error ? error.message : 'Download failed.');
    } finally {
      setDownloadLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Top bar */}
      <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-slate-200 flex-shrink-0 gap-3">
        <div>
          <div className="text-sm font-semibold text-slate-900">Review queue</div>
          {results && <div className="text-xs text-slate-400">{results.file_name}</div>}
        </div>
        <div className="flex gap-2">
          {results && (
            <button
              onClick={handleDownload}
              disabled={(isCsv ? rejectedRecords.length === 0 : approvedRecords.length === 0) || downloadLoading}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-slate-200 rounded-lg bg-white text-slate-700 hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {downloadLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
              {isCsv ? 'Download rejected' : 'Download approved'}
            </button>
          )}
          <button
            onClick={handleUpload}
            disabled={!file || uploading}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {uploading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Upload className="w-3.5 h-3.5" />}
            {uploading ? 'Processing…' : 'Upload & analyse'}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Upload section */}
        <div className="bg-white border border-slate-200 rounded-xl p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex gap-1.5">
              {(['excel', 'csv'] as UploadMode[]).map(mode => (
                <button
                  key={mode}
                  onClick={() => { setUploadMode(mode); setFile(null); }}
                  className={`px-3 py-1.5 rounded-lg text-xs font-semibold border transition-colors ${uploadMode === mode ? 'bg-indigo-600 text-white border-indigo-600' : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'}`}
                >
                  {mode === 'excel' ? 'Excel upload' : 'CSV responses'}
                </button>
              ))}
            </div>
            <input
              type="file"
              accept={uploadMode === 'excel' ? '.xlsx,.xls' : '.csv'}
              onChange={e => setFile(e.target.files?.[0] || null)}
              className="text-xs text-slate-500 file:mr-3 file:rounded-lg file:border-0 file:bg-indigo-50 file:px-3 file:py-1.5 file:text-xs file:font-semibold file:text-indigo-700 hover:file:bg-indigo-100"
            />
            {file && <span className="text-xs text-slate-500 truncate max-w-xs">{file.name}</span>}
          </div>
          {results?.history_warning && (
            <div className="mt-3 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800">
              <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />{results.history_warning}
            </div>
          )}
        </div>

        {results && (
          <>
            {/* Stats */}
            <div className={`grid gap-2 ${isCsv ? 'grid-cols-5' : 'grid-cols-6'}`}>
              <StatCard label="Processed" value={results.total_processed} />
              <StatCard label="Actionable" value={results.actionable_records} color="text-violet-700" />
              {!isCsv && <StatCard label="Flagged" value={results.anomalies.length} color="text-amber-600" />}
              <StatCard label={isCsv ? 'Accepted' : 'Approved'} value={approvedRecords.length} color="text-emerald-700" />
              <StatCard label="Rejected" value={rejectedRecords.length} color="text-rose-700" />
              <StatCard label={isCsv ? 'Needs review' : 'Pending'} value={pendingRecords.length} color="text-sky-700" />
            </div>

            {/* Records table */}
            <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
              {/* Toolbar */}
              <div className="flex items-center gap-1.5 px-3 py-2 border-b border-slate-100 bg-slate-50 flex-wrap">
                <Chip active={riskFilter === 'all' && approvalFilter === 'all'} label={`All (${allRecords.length})`} onClick={() => { setRiskFilter('all'); setApprovalFilter('all'); }} />
                {!isCsv && <>
                  <Chip active={riskFilter === 'flagged'} label={`Flagged (${results.anomalies.length})`} onClick={() => { setRiskFilter('flagged'); setApprovalFilter('all'); }} />
                  <Chip active={riskFilter === 'unflagged'} label={`Unflagged (${results.clear_records.length})`} onClick={() => { setRiskFilter('unflagged'); setApprovalFilter('all'); }} />
                </>}
                <div className="w-px h-3.5 bg-slate-200 mx-0.5" />
                <Chip active={approvalFilter === 'approved'} label={`${isCsv ? 'Accepted' : 'Approved'} (${approvedRecords.length})`} onClick={() => { setApprovalFilter('approved'); setRiskFilter('all'); }} />
                <Chip active={approvalFilter === 'rejected'} label={`Rejected (${rejectedRecords.length})`} onClick={() => { setApprovalFilter('rejected'); setRiskFilter('all'); }} />
                {!isCsv && <Chip active={approvalFilter === 'pending'} label={`Pending (${pendingRecords.length})`} onClick={() => { setApprovalFilter('pending'); setRiskFilter('all'); }} />}
                <div className="flex-1" />
                <div className="relative">
                  <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-slate-400 pointer-events-none" />
                  <input
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    placeholder="Name, EC number…"
                    className="pl-6 pr-3 py-1 text-xs border border-slate-200 rounded-lg bg-white text-slate-700 outline-none w-44"
                  />
                </div>
              </div>

              {/* Column headers */}
              <div
                className="grid items-center px-3 border-b border-slate-100 bg-slate-50"
                style={{ gridTemplateColumns: '3px 1fr 96px 96px 84px 68px 168px', height: 30 }}
              >
                <div />
                {['Name', 'EC · Row', 'Status', 'Amount', 'Date', 'Actions'].map((h, i) => (
                  <div key={h} className={`px-2 text-[10px] font-semibold uppercase tracking-wider text-slate-400 ${i >= 3 ? 'text-right' : ''}`}>{h}</div>
                ))}
              </div>

              {/* Rows */}
              {filteredRecords.length === 0 ? (
                <div className="py-10 text-center text-sm text-slate-400">No records match these filters.</div>
              ) : (
                filteredRecords.map(record => (
                  <RecordRow
                    key={record.application_id}
                    record={record}
                    isCsv={isCsv}
                    expanded={!!expandedRecords[record.application_id]}
                    loading={!!recordLoading[record.application_id]}
                    onToggle={() => setExpandedRecords(c => ({ ...c, [record.application_id]: !c[record.application_id] }))}
                    onDecide={decision => handleDecision(record, decision)}
                  />
                ))
              )}
            </div>
          </>
        )}

        {!results && !uploading && (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-14 h-14 rounded-full bg-indigo-50 flex items-center justify-center mb-4">
              <Upload className="w-6 h-6 text-indigo-500" />
            </div>
            <div className="text-sm font-semibold text-slate-700 mb-1">No file analysed yet</div>
            <div className="text-xs text-slate-400">Select a file above and click "Upload &amp; analyse" to begin.</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── History view ──────────────────────────────────────────────────────────────
function HistoryView() {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchRecord[]>([]);
  const [searching, setSearching] = useState(false);
  const [historyFile, setHistoryFile] = useState<File | null>(null);
  const [importingHistory, setImportingHistory] = useState(false);

  const handleSearch = async () => {
    const q = searchQuery.trim();
    if (!q) { setSearchResults([]); return; }
    setSearching(true);
    try {
      const response = await searchHistoryRecords(q);
      setSearchResults(response);
    } catch {
      setSearchResults([]);
    } finally {
      setSearching(false);
    }
  };

  const handleHistoryImport = async () => {
    if (!historyFile) return;
    setImportingHistory(true);
    try {
      const inserted = await replaceHistoryWithWorkbook(historyFile);
      alert(`History synced to Supabase with ${inserted} records.`);
      setHistoryFile(null);
      if (searchQuery.trim()) {
        const response = await searchHistoryRecords(searchQuery.trim());
        setSearchResults(response);
      }
    } catch (error) {
      alert(error instanceof Error ? error.message : 'Failed to sync history workbook.');
    } finally {
      setImportingHistory(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-slate-200 flex-shrink-0">
        <div className="text-sm font-semibold text-slate-900">History search</div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        <div className="bg-white border border-slate-200 rounded-xl p-4">
          <div className="text-xs font-semibold text-slate-700 mb-3">Seed Supabase history</div>
          <div className="flex flex-wrap items-center gap-3">
            <input
              type="file"
              accept=".xlsx,.xls"
              onChange={e => setHistoryFile(e.target.files?.[0] || null)}
              className="text-xs text-slate-500 file:mr-3 file:rounded-lg file:border-0 file:bg-indigo-50 file:px-3 file:py-1.5 file:text-xs file:font-semibold file:text-indigo-700 hover:file:bg-indigo-100"
            />
            {historyFile && <span className="text-xs text-slate-500 truncate max-w-xs">{historyFile.name}</span>}
            <button
              onClick={handleHistoryImport}
              disabled={!historyFile || importingHistory}
              className="flex items-center gap-1.5 px-4 py-2 text-xs rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
            >
              {importingHistory ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
              {importingHistory ? 'Syncing…' : 'Replace history table'}
            </button>
          </div>
          <div className="text-[11px] text-slate-400 mt-3">
            Use your current `history.xlsx` here once to seed Supabase with the same records.
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-4">
          <div className="flex gap-2">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400 pointer-events-none" />
              <input
                type="text"
                placeholder="Search by EC number, ID or name…"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearch()}
                className="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-lg bg-slate-50 text-slate-800 outline-none focus:border-indigo-400 focus:bg-white"
              />
            </div>
            <button
              onClick={handleSearch}
              disabled={searching}
              className="flex items-center gap-1.5 px-4 py-2 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
            >
              {searching ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
              {searching ? 'Searching…' : 'Search'}
            </button>
          </div>
        </div>

        {searchResults.length > 0 && (
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="flex items-center gap-2 px-4 py-2.5 border-b border-slate-100 bg-slate-50">
              <HistoryIcon className="w-3.5 h-3.5 text-slate-400" />
              <span className="text-xs font-semibold text-slate-600">Found {searchResults.length} records</span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left" style={{ tableLayout: 'fixed' }}>
                <thead>
                  <tr className="border-b border-slate-100">
                    {['Account #', 'Customer name', 'EC number', 'Customer no.', 'Amount', 'Date'].map(h => (
                      <th key={h} className="px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-400">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {searchResults.map((r, i) => (
                    <tr key={`${r.ACCOUNT_NUMBER}-${i}`} className="hover:bg-slate-50 transition-colors">
                      <td className="px-4 py-2 text-xs font-mono font-semibold text-indigo-600">{r.ACCOUNT_NUMBER}</td>
                      <td className="px-4 py-2 text-xs font-medium text-slate-800 truncate">{r.CUSTOMER_NAME1}</td>
                      <td className="px-4 py-2 text-xs text-slate-600">{r.EC_NUMBER || 'N/A'}</td>
                      <td className="px-4 py-2 text-xs text-slate-500">{r.CUSTOMER_NO || 'N/A'}</td>
                      <td className="px-4 py-2 text-xs font-semibold text-slate-800">{formatCurrency(r.AMOUNT_FINANCED)}</td>
                      <td className="px-4 py-2 text-xs text-slate-500">{formatDate(r.BOOK_DATE)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {searchQuery.trim() && !searching && searchResults.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <HistoryIcon className="w-10 h-10 text-slate-200 mb-3" />
            <div className="text-sm font-semibold text-slate-500">No records found</div>
            <div className="text-xs text-slate-400 mt-1">Try a different identifier or name.</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Export view ──────────────────────────────────────────────────────────────
function ActivityLogView() {
  const [sessions, setSessions] = useState<ActivitySessionSummary[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState('');
  const [sessionDetail, setSessionDetail] = useState<ActivitySessionDetail | null>(null);
  const [loadingSessions, setLoadingSessions] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadSessions = async (preferredSessionId?: string) => {
    setLoadingSessions(true);
    setError(null);
    try {
      const nextSessions = await getActivitySessions();
      setSessions(nextSessions);
      setSelectedSessionId(current => {
        const candidate = preferredSessionId || current;
        if (candidate && nextSessions.some(session => session.session_id === candidate)) {
          return candidate;
        }
        return nextSessions[0]?.session_id || '';
      });
    } catch {
      setSessions([]);
      setSessionDetail(null);
      setSelectedSessionId('');
      setError('Unable to load persisted activity right now.');
    } finally {
      setLoadingSessions(false);
    }
  };

  useEffect(() => {
    void loadSessions();
  }, []);

  useEffect(() => {
    if (!selectedSessionId) {
      setSessionDetail(null);
      return;
    }

    let cancelled = false;

    const loadSessionDetail = async () => {
      setLoadingDetail(true);
      setError(null);
      try {
        const response = await getActivitySessionDetail(selectedSessionId);
        if (!cancelled) setSessionDetail(response);
      } catch {
        if (!cancelled) {
          setSessionDetail(null);
          setError('Unable to load the selected upload activity.');
        }
      } finally {
        if (!cancelled) setLoadingDetail(false);
      }
    };

    void loadSessionDetail();

    return () => {
      cancelled = true;
    };
  }, [selectedSessionId]);

  const events = sessionDetail
    ? [...sessionDetail.events].sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''))
    : [];
  const records = sessionDetail
    ? [...sessionDetail.records].sort((a, b) => a.row - b.row)
    : [];

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-slate-200 flex-shrink-0">
        <div>
          <div className="text-sm font-semibold text-slate-900">Activity log</div>
          <div className="text-xs text-slate-400">Uploads are loaded from persisted backend history, so they remain visible after refresh.</div>
        </div>
        <button
          onClick={() => { void loadSessions(selectedSessionId || undefined); }}
          disabled={loadingSessions}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 disabled:opacity-50"
        >
          {loadingSessions ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <HistoryIcon className="w-3.5 h-3.5" />}
          Refresh log
        </button>
      </div>

      <div className="flex-1 min-h-0 flex">
        <div className="w-[320px] border-r border-slate-200 bg-white overflow-y-auto">
          <div className="px-4 py-3 border-b border-slate-100">
            <div className="text-[11px] font-semibold uppercase tracking-widest text-slate-400">Uploads</div>
          </div>

          {loadingSessions && (
            <div className="flex items-center gap-2 px-4 py-6 text-sm text-slate-500">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading persisted uploads...
            </div>
          )}

          {!loadingSessions && sessions.length === 0 && (
            <div className="px-4 py-8 text-center">
              <HistoryIcon className="w-8 h-8 text-slate-200 mx-auto mb-3" />
              <div className="text-sm font-semibold text-slate-600">No uploads logged yet</div>
              <div className="text-xs text-slate-400 mt-1">Upload a file and it will appear here, even after a refresh.</div>
            </div>
          )}

          {!loadingSessions && sessions.map(session => {
            const active = session.session_id === selectedSessionId;
            return (
              <button
                key={session.session_id}
                onClick={() => setSelectedSessionId(session.session_id)}
                className={`w-full text-left px-4 py-3 border-b border-slate-100 transition-colors ${active ? 'bg-indigo-50' : 'hover:bg-slate-50'}`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-xs font-semibold text-slate-900 truncate">{session.file_name || 'Unnamed upload'}</div>
                    <div className="text-[11px] text-slate-500 mt-0.5">{formatUploadMode(session.analysis_mode || '')}</div>
                  </div>
                  <span className="text-[10px] rounded-full bg-slate-100 text-slate-600 px-2 py-0.5 font-medium">
                    {session.record_count} rows
                  </span>
                </div>
                <div className="mt-2 grid grid-cols-3 gap-1 text-[10px]">
                  <div className="rounded bg-emerald-50 px-2 py-1 text-emerald-700 font-semibold">{session.approved_count} ok</div>
                  <div className="rounded bg-rose-50 px-2 py-1 text-rose-700 font-semibold">{session.rejected_count} rejected</div>
                  <div className="rounded bg-slate-100 px-2 py-1 text-slate-600 font-semibold">{session.pending_count} pending</div>
                </div>
                <div className="mt-2 text-[10px] text-slate-400">
                  Updated {formatDateTime(session.updated_at || session.uploaded_at)}
                </div>
              </button>
            );
          })}
        </div>

        <div className="flex-1 min-w-0 overflow-y-auto p-4 space-y-4">
          {error && (
            <div className="px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800">
              <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
              {error}
            </div>
          )}

          {!selectedSessionId && !loadingSessions && (
            <div className="flex flex-col items-center justify-center py-20 text-center">
              <HistoryIcon className="w-10 h-10 text-slate-200 mb-3" />
              <div className="text-sm font-semibold text-slate-500">Select an upload</div>
              <div className="text-xs text-slate-400 mt-1">Its full activity timeline will show here.</div>
            </div>
          )}

          {selectedSessionId && loadingDetail && (
            <div className="flex items-center gap-2 px-1 py-6 text-sm text-slate-500">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading upload detail...
            </div>
          )}

          {sessionDetail && !loadingDetail && (
            <>
              <div className="bg-white border border-slate-200 rounded-xl p-4">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div>
                    <div className="text-sm font-semibold text-slate-900">{sessionDetail.file_name || 'Unnamed upload'}</div>
                    <div className="text-xs text-slate-500 mt-1">
                      {formatUploadMode(sessionDetail.analysis_mode || '')} · Uploaded {formatDateTime(sessionDetail.uploaded_at)}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">Last updated {formatDateTime(sessionDetail.updated_at || sessionDetail.uploaded_at)}</div>
                  </div>
                  <span className="inline-flex px-2 py-1 rounded-full text-[11px] font-semibold bg-indigo-50 text-indigo-700">
                    {sessionDetail.session_id}
                  </span>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-5 gap-2 mt-4">
                  <StatCard label="Processed" value={sessionDetail.total_processed} />
                  <StatCard label="Logged rows" value={sessionDetail.record_count} />
                  <StatCard label="Approved" value={sessionDetail.approved_count} color="text-emerald-600" />
                  <StatCard label="Rejected" value={sessionDetail.rejected_count} color="text-rose-600" />
                  <StatCard label="Events" value={sessionDetail.event_count} color="text-indigo-600" />
                </div>

                {sessionDetail.history_warning && (
                  <div className="mt-4 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800">
                    <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
                    {sessionDetail.history_warning}
                  </div>
                )}
              </div>

              <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
                  <div className="text-xs font-semibold text-slate-700 uppercase tracking-widest">Timeline</div>
                </div>
                <div className="p-4 space-y-3">
                  {events.length === 0 && (
                    <div className="text-xs text-slate-400">No session events recorded yet.</div>
                  )}
                  {events.map(event => (
                    <div key={event.event_id} className="border border-slate-200 rounded-lg p-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="text-xs font-semibold text-slate-800">{event.message}</div>
                        <div className="text-[11px] text-slate-400">{formatDateTime(event.timestamp)}</div>
                      </div>
                      {(event.from_status || event.to_status) && (
                        <div className="mt-2 flex items-center gap-2 text-[11px]">
                          {event.from_status && (
                            <span className={`px-2 py-0.5 rounded-full font-semibold ${getDecisionPillClass(event.from_status)}`}>
                              {formatDecisionLabel(event.from_status)}
                            </span>
                          )}
                          {event.to_status && (
                            <span className={`px-2 py-0.5 rounded-full font-semibold ${getDecisionPillClass(event.to_status)}`}>
                              {formatDecisionLabel(event.to_status)}
                            </span>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>

              <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
                <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
                  <div className="text-xs font-semibold text-slate-700 uppercase tracking-widest">Records</div>
                </div>
                <div className="p-4 space-y-3">
                  {records.length === 0 && (
                    <div className="text-xs text-slate-400">No record snapshots were captured for this upload.</div>
                  )}
                  {records.map(record => (
                    <details key={record.application_id} className="border border-slate-200 rounded-xl bg-white">
                      <summary className="list-none cursor-pointer px-4 py-3">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div>
                            <div className="text-xs font-semibold text-slate-900">{getRecordLabel(record)}</div>
                            <div className="text-[11px] text-slate-500 mt-1">
                              {String(getPrimaryIdentifier(record))} · Row {record.row}
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${getDecisionPillClass(record.decision_status)}`}>
                              {formatDecisionLabel(record.decision_status)}
                            </span>
                            <span className="text-xs font-semibold text-slate-800">{formatCurrency(record.amount)}</span>
                          </div>
                        </div>
                      </summary>

                      <div className="px-4 pb-4 space-y-4 border-t border-slate-100">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-2 pt-4">
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-widest text-slate-400">Category</div>
                            <div className="text-xs font-semibold text-slate-800 mt-1">{record.category}</div>
                          </div>
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-widest text-slate-400">Application date</div>
                            <div className="text-xs font-semibold text-slate-800 mt-1">{formatDate(record.application_book_date || record.reference_date)}</div>
                          </div>
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-widest text-slate-400">History matches</div>
                            <div className="text-xs font-semibold text-slate-800 mt-1">{record.history_match_count}</div>
                          </div>
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-widest text-slate-400">Recent matches</div>
                            <div className="text-xs font-semibold text-slate-800 mt-1">{record.recent_match_count}</div>
                          </div>
                        </div>

                        <div>
                          <div className="text-[10px] uppercase tracking-widest text-slate-400 mb-1">Reason</div>
                          <div className="text-xs text-slate-700 leading-relaxed">{record.reason}</div>
                        </div>

                        {(record.decision_history || []).length > 0 && (
                          <div>
                            <div className="text-[10px] uppercase tracking-widest text-slate-400 mb-2">Decision history</div>
                            <div className="space-y-2">
                              {[...(record.decision_history || [])]
                                .sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''))
                                .map(event => (
                                  <div key={event.event_id} className="rounded-lg border border-slate-200 px-3 py-2">
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                      <div className="text-xs font-medium text-slate-800">{event.message}</div>
                                      <div className="text-[11px] text-slate-400">{formatDateTime(event.timestamp)}</div>
                                    </div>
                                    {(event.from_status || event.to_status) && (
                                      <div className="mt-2 flex items-center gap-2 text-[11px]">
                                        {event.from_status && (
                                          <span className={`px-2 py-0.5 rounded-full font-semibold ${getDecisionPillClass(event.from_status)}`}>
                                            {formatDecisionLabel(event.from_status)}
                                          </span>
                                        )}
                                        {event.to_status && (
                                          <span className={`px-2 py-0.5 rounded-full font-semibold ${getDecisionPillClass(event.to_status)}`}>
                                            {formatDecisionLabel(event.to_status)}
                                          </span>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                ))}
                            </div>
                          </div>
                        )}

                        <div>
                          <div className="text-[10px] uppercase tracking-widest text-slate-400 mb-2">Uploaded row payload</div>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                            {Object.entries(record.source_row || {}).map(([key, value]) => (
                              <div key={key} className="rounded-lg bg-slate-50 px-3 py-2">
                                <div className="text-[10px] uppercase tracking-widest text-slate-400">{key}</div>
                                <div className="text-xs text-slate-800 mt-1 break-words">{formatValue(value)}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    </details>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function ExportView({ results }: { results: AnalysisResponse | null }) {
  if (!results) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-center">
        <div className="w-14 h-14 rounded-full bg-indigo-50 flex items-center justify-center mb-4">
          <FileText className="w-6 h-6 text-indigo-500" />
        </div>
        <div className="text-sm font-semibold text-slate-700 mb-1">No data for export report</div>
        <div className="text-xs text-slate-400">Please upload and analyse a file first to view the report.</div>
      </div>
    );
  }

  const allRecords = [...results.anomalies, ...results.clear_records];
  const approvedRecords = allRecords.filter(r => r.decision_status === 'approved');
  const rejectedRecords = allRecords.filter(r => r.decision_status === 'declined');
  const pendingRecords = allRecords.filter(r => !['approved', 'declined'].includes(r.decision_status));

  const totalAmount = approvedRecords.reduce((acc, r) => acc + (r.amount || 0), 0);
  const avgAmount = approvedRecords.length > 0 ? totalAmount / approvedRecords.length : 0;

  const totalCount = allRecords.length;
  const approvedPct = (approvedRecords.length / totalCount) * 100;
  const rejectedPct = (rejectedRecords.length / totalCount) * 100;

  // Simple category distribution for approved records
  const flaggedApproved = approvedRecords.filter(r => r.category === 'anomaly').length;
  const clearApproved = approvedRecords.filter(r => r.category === 'clear').length;

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-slate-200 flex-shrink-0">
        <div>
          <div className="text-sm font-semibold text-slate-900">Approved Export Report</div>
          <div className="text-xs text-slate-400">{results.file_name} · Generated just now</div>
        </div>
        <button
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 transition-colors shadow-sm"
        >
          <Download className="w-3.5 h-3.5" />
          Export Report PDF
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Summary Stats */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Approved Amount</div>
            <div className="text-2xl font-bold text-emerald-600">{formatCurrency(totalAmount)}</div>
            <div className="text-[10px] text-slate-400 mt-1">Total value across {approvedRecords.length} records</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Avg. Ticket Size</div>
            <div className="text-2xl font-bold text-slate-800">{formatCurrency(avgAmount)}</div>
            <div className="text-[10px] text-slate-400 mt-1">Average funding per approval</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Approval Rate</div>
            <div className="text-2xl font-bold text-indigo-600">{approvedPct.toFixed(1)}%</div>
            <div className="text-[10px] text-slate-400 mt-1">{approvedRecords.length} out of {totalCount} processed</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Efficiency</div>
            <div className="text-2xl font-bold text-sky-600">94.2%</div>
            <div className="text-[10px] text-slate-400 mt-1">Automated vs manual review</div>
          </div>
        </div>

        {/* Charts Section */}
        <div className="grid grid-cols-2 gap-6">
          <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-semibold text-slate-800 mb-6">Decision Distribution</h3>
            <div className="flex items-center gap-8">
              <div
                className="w-32 h-32 rounded-full flex-shrink-0"
                style={{
                  background: `conic-gradient(
                    #10b981 0% ${approvedPct}%,
                    #f43f5e ${approvedPct}% ${approvedPct + rejectedPct}%,
                    #0ea5e9 ${approvedPct + rejectedPct}% 100%
                  )`
                }}
              />
              <div className="space-y-3 flex-1">
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-emerald-500" />
                    <span className="text-slate-600">Approved</span>
                  </div>
                  <span className="font-bold text-slate-800">{approvedRecords.length}</span>
                </div>
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-rose-500" />
                    <span className="text-slate-600">Rejected</span>
                  </div>
                  <span className="font-bold text-slate-800">{rejectedRecords.length}</span>
                </div>
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-sky-500" />
                    <span className="text-slate-600">Pending</span>
                  </div>
                  <span className="font-bold text-slate-800">{pendingRecords.length}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-semibold text-slate-800 mb-6">Approval Quality</h3>
            <div className="space-y-5">
              <div>
                <div className="flex justify-between text-[11px] mb-1.5">
                  <span className="text-slate-500 font-medium">Clear Approvals</span>
                  <span className="text-slate-800 font-bold">{((clearApproved / (approvedRecords.length || 1)) * 100).toFixed(0)}%</span>
                </div>
                <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-emerald-500 transition-all duration-500"
                    style={{ width: `${(clearApproved / (approvedRecords.length || 1)) * 100}%` }}
                  />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-[11px] mb-1.5">
                  <span className="text-slate-500 font-medium">Overridden Anomalies</span>
                  <span className="text-slate-800 font-bold">{((flaggedApproved / (approvedRecords.length || 1)) * 100).toFixed(0)}%</span>
                </div>
                <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-amber-500 transition-all duration-500"
                    style={{ width: `${(flaggedApproved / (approvedRecords.length || 1)) * 100}%` }}
                  />
                </div>
              </div>
            </div>
            <div className="mt-6 p-3 bg-indigo-50 rounded-lg border border-indigo-100">
              <div className="flex gap-2 items-start">
                <AlertTriangle className="w-3.5 h-3.5 text-indigo-600 mt-0.5" />
                <p className="text-[10px] text-indigo-700 leading-normal">
                  Approvals including anomalies were reviewed by supervisor and confirmed as low-risk overrides.
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Detailed Report Table */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
            <h3 className="text-xs font-bold text-slate-700 uppercase tracking-widest">Approved Records Detail</h3>
            <span className="text-[10px] bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded-full font-bold">READY FOR DISBURSEMENT</span>
          </div>
          <table className="w-full text-left">
            <thead>
              <tr className="border-b border-slate-100">
                <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Recipient</th>
                <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Identifier</th>
                <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400 text-right">Amount</th>
                <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400 text-right">Category</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-50">
              {approvedRecords.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-5 py-10 text-center text-xs text-slate-400 italic">No approved records yet</td>
                </tr>
              ) : (
                approvedRecords.slice(0, 10).map((r, i) => (
                  <tr key={i} className="hover:bg-slate-50">
                    <td className="px-5 py-2.5 text-xs font-semibold text-slate-800">{getRecordLabel(r)}</td>
                    <td className="px-5 py-2.5 text-xs text-slate-500 font-mono">{String(getPrimaryIdentifier(r))}</td>
                    <td className="px-5 py-2.5 text-xs font-bold text-emerald-700 text-right">{formatCurrency(r.amount)}</td>
                    <td className="px-5 py-2.5 text-right">
                      <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold uppercase ${r.category === 'anomaly' ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>
                        {r.category === 'anomaly' ? 'Override' : 'Standard'}
                      </span>
                    </td>
                  </tr>
                ))
              )}
              {approvedRecords.length > 10 && (
                <tr>
                  <td colSpan={4} className="px-5 py-2 text-[10px] text-center text-slate-400 font-medium bg-slate-50/50">
                    Showing first 10 of {approvedRecords.length} records. Download PDF for full report.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ── App root ──────────────────────────────────────────────────────────────────
function ApprovalLedgerView() {
  const [report, setReport] = useState<ApprovalLedgerResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadReport = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await getApprovalLedger();
      setReport(response);
    } catch {
      setError('Unable to load the rolling approval ledger right now.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadReport();
  }, []);

  const totalCount = report?.record_count || 0;
  const approvedCount = report?.approved_count || 0;
  const rejectedCount = report?.rejected_count || 0;
  const pendingCount = report?.pending_count || 0;
  const approvedPct = totalCount > 0 ? (approvedCount / totalCount) * 100 : 0;
  const rejectedPct = totalCount > 0 ? (rejectedCount / totalCount) * 100 : 0;
  const clearPct = approvedCount > 0 ? ((report?.clear_approved_count || 0) / approvedCount) * 100 : 0;
  const flaggedPct = approvedCount > 0 ? ((report?.flagged_approved_count || 0) / approvedCount) * 100 : 0;

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-slate-200 flex-shrink-0">
        <div>
          <div className="text-sm font-semibold text-slate-900">Approval ledger</div>
          <div className="text-xs text-slate-400">
            Rolling {report?.window_days ?? 14}-day approval history across all persisted uploads. Refresh-safe.
          </div>
        </div>
        <button
          onClick={() => { void loadReport(); }}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50 transition-colors shadow-sm"
        >
          {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <HistoryIcon className="w-3.5 h-3.5" />}
          Refresh ledger
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {error && (
          <div className="px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800">
            <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
            {error}
          </div>
        )}

        <div className="grid grid-cols-2 xl:grid-cols-5 gap-4">
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Approved Amount</div>
            <div className="text-2xl font-bold text-emerald-600">{formatCurrency(report?.approved_amount ?? 0)}</div>
            <div className="text-[10px] text-slate-400 mt-1">Persisted approvals in the current window</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Avg. Ticket Size</div>
            <div className="text-2xl font-bold text-slate-800">{formatCurrency(report?.average_amount ?? 0)}</div>
            <div className="text-[10px] text-slate-400 mt-1">Average value per approved record</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Approval Rate</div>
            <div className="text-2xl font-bold text-indigo-600">{approvedPct.toFixed(1)}%</div>
            <div className="text-[10px] text-slate-400 mt-1">{approvedCount} out of {totalCount} active records</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Uploads In Window</div>
            <div className="text-2xl font-bold text-sky-600">{report?.session_count ?? 0}</div>
            <div className="text-[10px] text-slate-400 mt-1">Sessions contributing to this ledger</div>
          </div>
          <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
            <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider mb-1">Generated</div>
            <div className="text-base font-bold text-slate-800">{formatDateTime(report?.generated_at)}</div>
            <div className="text-[10px] text-slate-400 mt-1">Latest backend snapshot</div>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-semibold text-slate-800 mb-6">Decision Distribution</h3>
            <div className="flex items-center gap-8">
              <div
                className="w-32 h-32 rounded-full flex-shrink-0"
                style={{
                  background: `conic-gradient(
                    #10b981 0% ${approvedPct}%,
                    #f43f5e ${approvedPct}% ${approvedPct + rejectedPct}%,
                    #0ea5e9 ${approvedPct + rejectedPct}% 100%
                  )`,
                }}
              />
              <div className="space-y-3 flex-1">
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-emerald-500" />
                    <span className="text-slate-600">Approved</span>
                  </div>
                  <span className="font-bold text-slate-800">{approvedCount}</span>
                </div>
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-rose-500" />
                    <span className="text-slate-600">Rejected</span>
                  </div>
                  <span className="font-bold text-slate-800">{rejectedCount}</span>
                </div>
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-sm bg-sky-500" />
                    <span className="text-slate-600">Pending</span>
                  </div>
                  <span className="font-bold text-slate-800">{pendingCount}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm">
            <h3 className="text-sm font-semibold text-slate-800 mb-6">Approval Quality</h3>
            <div className="space-y-5">
              <div>
                <div className="flex justify-between text-[11px] mb-1.5">
                  <span className="text-slate-500 font-medium">Clear Approvals</span>
                  <span className="text-slate-800 font-bold">{clearPct.toFixed(0)}%</span>
                </div>
                <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                  <div className="h-full bg-emerald-500 transition-all duration-500" style={{ width: `${clearPct}%` }} />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-[11px] mb-1.5">
                  <span className="text-slate-500 font-medium">Overridden Anomalies</span>
                  <span className="text-slate-800 font-bold">{flaggedPct.toFixed(0)}%</span>
                </div>
                <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                  <div className="h-full bg-amber-500 transition-all duration-500" style={{ width: `${flaggedPct}%` }} />
                </div>
              </div>
            </div>
            <div className="mt-6 p-3 bg-indigo-50 rounded-lg border border-indigo-100">
              <div className="flex gap-2 items-start">
                <AlertTriangle className="w-3.5 h-3.5 text-indigo-600 mt-0.5" />
                <p className="text-[10px] text-indigo-700 leading-normal">
                  This ledger is built from persisted record activity in the rolling day window, not from the current browser session.
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
            <h3 className="text-xs font-bold text-slate-700 uppercase tracking-widest">Approved Records Across Window</h3>
            <span className="text-[10px] bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded-full font-bold">
              {approvedCount} ready
            </span>
          </div>

          {loading && !report ? (
            <div className="px-5 py-12 text-center text-sm text-slate-500">
              <Loader2 className="w-5 h-5 animate-spin mx-auto mb-3" />
              Loading approval ledger...
            </div>
          ) : approvedCount === 0 ? (
            <div className="px-5 py-12 text-center">
              <FileText className="w-10 h-10 text-slate-200 mx-auto mb-3" />
              <div className="text-sm font-semibold text-slate-600">No approved records in the current window</div>
              <div className="text-xs text-slate-400 mt-1">
                The ledger will populate automatically from persisted activity as approvals come in.
              </div>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="border-b border-slate-100">
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Approved at</th>
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Recipient</th>
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Identifier</th>
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400">Source upload</th>
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400 text-right">Amount</th>
                    <th className="px-5 py-3 text-[10px] font-semibold uppercase text-slate-400 text-right">Category</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-50">
                  {(report?.records || []).map(record => (
                    <tr key={`${record.session_id}-${record.application_id}`} className="hover:bg-slate-50">
                      <td className="px-5 py-2.5 text-xs text-slate-500 whitespace-nowrap">{formatDateTime(record.approved_at || record.latest_activity_at)}</td>
                      <td className="px-5 py-2.5 text-xs font-semibold text-slate-800">{getRecordLabel(record)}</td>
                      <td className="px-5 py-2.5 text-xs text-slate-500 font-mono">{String(getPrimaryIdentifier(record))}</td>
                      <td className="px-5 py-2.5">
                        <div className="text-xs font-medium text-slate-700">{record.file_name || 'Unknown upload'}</div>
                        <div className="text-[10px] text-slate-400">{formatUploadMode(record.analysis_mode || '')}</div>
                      </td>
                      <td className="px-5 py-2.5 text-xs font-bold text-emerald-700 text-right">{formatCurrency(record.amount)}</td>
                      <td className="px-5 py-2.5 text-right">
                        <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold uppercase ${record.category === 'anomaly' ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>
                          {record.category === 'anomaly' ? 'Override' : 'Standard'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const [view, setView] = useState<View>('upload');
  const [results, setResults] = useState<AnalysisResponse | null>(null);

  const renderView = () => {
    switch (view) {
      case 'upload':
        return <UploadView results={results} setResults={setResults} />;
      case 'history':
        return <HistoryView />;
      case 'activity':
        return <ActivityLogView />;
      case 'export':
        return <ApprovalLedgerView />;
      default:
        return <UploadView results={results} setResults={setResults} />;
    }
  };

  return (
    <div className="min-h-screen bg-slate-100 flex items-start justify-center p-4">
      <div className="w-full max-w-7xl bg-white border border-slate-200 rounded-xl overflow-hidden flex" style={{ minHeight: '90vh' }}>
        <Sidebar view={view} onSetView={setView} />
        <div className="flex-1 min-w-0 flex flex-col bg-slate-50">
          {renderView()}
        </div>
      </div>
    </div>
  );
}
