-- KPI: Bug Issue Resolution by Time Bucket
-- Definition: count of bug issues per resolution time bucket per month
-- Excludes: bot authors (filtered upstream in fct)
-- Grain: one row per calendar month (Europe/Amsterdam)
-- Note: buckets are mutually exclusive and exhaustive
--       (resolved_within_24h + _7d + _14d + _30d + over_30d + still_open = total_bug_count)

with base as (
    select
        issue_id,
        created_at,
        resolution_bucket
    from {{ ref('fct_github__issues') }}
    where
        is_bug = true
        -- excluded closed issues with reason other than 'completed'
        -- to avoid counting closed as duplicates issues and others
        and (is_completed or is_open)
        and created_at::date >= '{{ var("kpi_start_date") }}'::date
),

aggregated as (
    select
        date_trunc('week', created_at)::date as creation_week,
        resolution_bucket,
        count(*) as bug_count
    from base
    group by 1,2
)

select * from aggregated
order by creation_week desc
