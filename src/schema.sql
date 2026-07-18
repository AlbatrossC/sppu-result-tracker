-- Minimal SPPU Result Tracker schema for PostgreSQL/Neon.
-- Run this after deployment to keep only the live mirror and history tables.

drop table if exists public.notification_outbox;
drop table if exists public.pending_changes;
drop table if exists public.tracker_state;
drop table if exists public.tracker_runs;

create table if not exists public.results (
    id bigserial primary key,
    course_key text not null,
    course_name text not null,
    result_date date not null,
    notification_sent boolean not null default false,
    first_seen timestamptz not null default now(),
    last_seen timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.results add column if not exists course_key text;
alter table public.results add column if not exists notification_sent boolean default true;
alter table public.results alter column notification_sent set default false;
alter table public.results alter column notification_sent set not null;

update public.results
set course_key = lower(regexp_replace(trim(course_name), '\s+', ' ', 'g'))
where course_key is null;

do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public'
          and table_name = 'results'
          and column_name = 'is_active'
    ) then
        execute 'delete from public.results where is_active = false';
        execute 'alter table public.results drop column is_active';
    end if;
end $$;

alter table public.results alter column course_key set not null;

create unique index if not exists results_course_key_date_unique
    on public.results (course_key, result_date);
create index if not exists idx_results_date
    on public.results (result_date desc);
create index if not exists idx_results_last_seen
    on public.results (last_seen desc);
create index if not exists idx_results_unsent
    on public.results (updated_at desc)
    where notification_sent = false;

create table if not exists public.results_history (
    id bigserial primary key,
    result_id bigint,
    course_key text not null,
    course_name text not null,
    change_type text not null check (change_type in ('added', 'updated', 'removed')),
    old_result_date date,
    new_result_date date,
    notification_sent boolean not null default false,
    notification_error text,
    created_at timestamptz not null default now()
);

alter table public.results_history add column if not exists result_id bigint;
alter table public.results_history add column if not exists course_key text;
alter table public.results_history add column if not exists old_result_date date;
alter table public.results_history add column if not exists new_result_date date;
alter table public.results_history add column if not exists notification_sent boolean default true;
alter table public.results_history add column if not exists notification_error text;
alter table public.results_history alter column notification_sent set default false;
alter table public.results_history alter column notification_sent set not null;

do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public'
          and table_name = 'results_history'
          and column_name = 'previous_date'
    ) then
        execute 'update public.results_history set old_result_date = previous_date where old_result_date is null';
        execute 'alter table public.results_history drop column previous_date';
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public'
          and table_name = 'results_history'
          and column_name = 'result_date'
    ) then
        execute 'update public.results_history set new_result_date = result_date where new_result_date is null';
        execute 'alter table public.results_history drop column result_date';
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public'
          and table_name = 'results_history'
          and column_name = 'run_id'
    ) then
        execute 'alter table public.results_history drop column run_id';
    end if;
end $$;

update public.results_history
set course_key = lower(regexp_replace(trim(course_name), '\s+', ' ', 'g'))
where course_key is null;

alter table public.results_history alter column course_key set not null;

create index if not exists idx_results_history_created
    on public.results_history (created_at desc);
create index if not exists idx_results_history_course
    on public.results_history (course_key, created_at desc);
create index if not exists idx_results_history_unsent
    on public.results_history (created_at, id)
    where notification_sent = false;

comment on table public.results is 'Current SPPU result page mirror.';
comment on table public.results_history is 'Permanent result change history and notification state.';
