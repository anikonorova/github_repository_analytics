-- KPI: Bug Issue Resolution Time
-- Definition: hours from issue opened to closed, for bug-labelled issues only
-- Excludes: bot authors (filtered upstream in fct) and closed issues with reason other than 'completed'
-- to avoid counting closed as duplicates issues and others
-- Grain: one row per quarter (Europe/Amsterdam timezone)

with base as (
    select
        issue_id,
        created_at,
        hours_to_close,
        days_to_close
    from {{ ref('fct_github__issues') }}
    where
        is_bug = true
        and is_completed = true
        and created_at::date >= '{{ var("kpi_start_date") }}'::date
),

aggregated as (
    select
        date_trunc('quarter', created_at)::date as creation_quarter,
        count(*) as resolved_bug_count,
        median(hours_to_close) as median_hours_to_resolve,
        avg(hours_to_close) as mean_hours_to_resolve,
        sum(case when days_to_close <= 7 then 1 else 0 end)/
            count(*) as pct_resolved_within_7d
    from base
    group by 1
)

select * from aggregated
order by creation_quarter desc
