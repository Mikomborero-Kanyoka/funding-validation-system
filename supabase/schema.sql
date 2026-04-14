create extension if not exists pgcrypto;

create table if not exists public.review_sessions (
  id uuid primary key default gen_random_uuid(),
  analysis_mode text not null check (analysis_mode in ('excel', 'csv')),
  file_name text not null,
  columns jsonb not null default '[]'::jsonb,
  total_processed integer not null default 0,
  actionable_records integer not null default 0,
  approved_count integer not null default 0,
  rejected_count integer not null default 0,
  pending_count integer not null default 0,
  event_count integer not null default 0,
  history_warning text,
  uploaded_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.review_records (
  id uuid primary key default gen_random_uuid(),
  session_id uuid not null references public.review_sessions(id) on delete cascade,
  application_id text not null,
  row_number integer not null,
  applicant_name text,
  ec_number text,
  customer_no text,
  amount numeric,
  application_book_date timestamptz,
  category text not null check (category in ('anomaly', 'clear')),
  reason text not null,
  anomaly_reasons jsonb not null default '[]'::jsonb,
  reference_date timestamptz,
  history_match_count integer not null default 0,
  recent_match_count integer not null default 0,
  latest_book_date timestamptz,
  matched_records jsonb not null default '[]'::jsonb,
  decision_status text not null check (decision_status in ('pending', 'approved', 'declined', 'manual_review')),
  response_status text,
  source_row jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (session_id, application_id)
);

create table if not exists public.activity_events (
  id uuid primary key default gen_random_uuid(),
  session_id uuid not null references public.review_sessions(id) on delete cascade,
  application_id text,
  event_type text not null,
  record_label text,
  from_status text,
  to_status text,
  message text not null,
  reason text,
  response_status text,
  created_at timestamptz not null default now()
);

create table if not exists public.history_records (
  id uuid primary key default gen_random_uuid(),
  source_session_id uuid references public.review_sessions(id) on delete set null,
  source_application_id text,
  import_row_number integer,
  account_number text,
  customer_name1 text,
  ec_number text,
  customer_no text,
  amount_financed numeric,
  book_date timestamptz,
  normalized_ec_number text,
  normalized_customer_no text,
  row_data jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_review_records_session_id on public.review_records(session_id);
create index if not exists idx_review_records_decision_status on public.review_records(decision_status);
create index if not exists idx_review_records_updated_at on public.review_records(updated_at desc);
create index if not exists idx_activity_events_session_id on public.activity_events(session_id);
create index if not exists idx_activity_events_created_at on public.activity_events(created_at desc);
create index if not exists idx_history_records_book_date on public.history_records(book_date desc);
create index if not exists idx_history_records_norm_ec on public.history_records(normalized_ec_number);
create index if not exists idx_history_records_norm_customer on public.history_records(normalized_customer_no);
create index if not exists idx_history_records_customer_name on public.history_records(customer_name1);

alter table public.review_sessions enable row level security;
alter table public.review_records enable row level security;
alter table public.activity_events enable row level security;
alter table public.history_records enable row level security;

drop policy if exists "public full access review_sessions" on public.review_sessions;
create policy "public full access review_sessions"
  on public.review_sessions
  for all
  using (true)
  with check (true);

drop policy if exists "public full access review_records" on public.review_records;
create policy "public full access review_records"
  on public.review_records
  for all
  using (true)
  with check (true);

drop policy if exists "public full access activity_events" on public.activity_events;
create policy "public full access activity_events"
  on public.activity_events
  for all
  using (true)
  with check (true);

drop policy if exists "public full access history_records" on public.history_records;
create policy "public full access history_records"
  on public.history_records
  for all
  using (true)
  with check (true);
