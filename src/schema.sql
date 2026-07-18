-- SPPU Result Tracker schema for PostgreSQL/Neon.
-- Paste this entire file into the Neon SQL Editor and run it once.
-- It does not delete existing result data.

create extension if not exists pgcrypto;

create table if not exists public.results (
    id bigserial primary key,
    course_key text not null,
    course_name text not null,
    result_date date not null,
    is_active boolean not null default true,
    first_seen timestamptz not null default now(),
    last_seen timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

-- Compatibility with an earlier version of schema.sql.
alter table public.results add column if not exists course_key text;
update public.results
set course_key = lower(regexp_replace(trim(course_name), '\s+', ' ', 'g'))
where course_key is null;
alter table public.results alter column course_key set not null;

create unique index if not exists results_course_key_date_unique
    on public.results (course_key, result_date);
create index if not exists idx_results_active_date
    on public.results (result_date desc) where is_active = true;
create index if not exists idx_results_last_seen
    on public.results (last_seen desc);

create table if not exists public.tracker_runs (
    run_id uuid primary key,
    started_at timestamptz not null,
    finished_at timestamptz,
    status text not null,
    parsed_count integer not null default 0,
    added_count integer not null default 0,
    updated_count integer not null default 0,
    removed_count integer not null default 0,
    baseline_created boolean not null default false,
    snapshot_hash text,
    error_message text
);

create index if not exists idx_tracker_runs_started
    on public.tracker_runs (started_at desc);
create index if not exists idx_tracker_runs_status
    on public.tracker_runs (status, started_at desc);

create table if not exists public.results_history (
    id bigserial primary key,
    run_id uuid,
    course_key text not null,
    course_name text not null,
    result_date date,
    change_type text not null check (change_type in ('added', 'updated', 'removed')),
    previous_date date,
    created_at timestamptz not null default now()
);

alter table public.results_history add column if not exists run_id uuid;
alter table public.results_history add column if not exists course_key text;
update public.results_history
set course_key = lower(regexp_replace(trim(course_name), '\s+', ' ', 'g'))
where course_key is null;
alter table public.results_history alter column course_key set not null;

create index if not exists idx_results_history_created
    on public.results_history (created_at desc);
create index if not exists idx_results_history_course
    on public.results_history (course_key, created_at desc);

create table if not exists public.tracker_state (
    id boolean primary key default true check (id = true),
    initialized boolean not null default false,
    last_successful_run_id uuid,
    last_snapshot_count integer,
    last_snapshot_hash text,
    pending_snapshot_hash text,
    pending_snapshot_count integer,
    consecutive_failures integer not null default 0,
    outage_alerted boolean not null default false,
    updated_at timestamptz not null default now()
);

insert into public.tracker_state (id)
values (true)
on conflict (id) do nothing;

create table if not exists public.pending_changes (
    candidate_key text primary key,
    change_type text not null check (change_type in ('updated', 'removed')),
    course_key text not null,
    course_name text not null,
    old_date date,
    new_date date,
    first_seen_run_id uuid not null,
    last_seen_run_id uuid not null,
    observations integer not null default 1 check (observations > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_pending_changes_course
    on public.pending_changes (course_key);

create table if not exists public.notification_outbox (
    id uuid primary key default gen_random_uuid(),
    run_id uuid,
    event_type text not null,
    payload jsonb not null,
    status text not null default 'pending' check (status in ('pending', 'sent', 'dead')),
    attempts integer not null default 0 check (attempts >= 0),
    next_attempt_at timestamptz not null default now(),
    discord_message_id text,
    last_error text,
    created_at timestamptz not null default now(),
    sent_at timestamptz
);

create index if not exists idx_outbox_pending
    on public.notification_outbox (next_attempt_at, created_at)
    where status = 'pending';
create index if not exists idx_outbox_dead
    on public.notification_outbox (created_at desc)
    where status = 'dead';

comment on table public.results is 'Authoritative current and historical SPPU result pairs.';
comment on table public.results_history is 'Immutable confirmed result changes.';
comment on table public.notification_outbox is 'Durable Discord delivery queue.';
