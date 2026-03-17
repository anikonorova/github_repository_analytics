-- KPI: Time to First Review
-- Definition: hours from PR opened to first human review
-- Excludes: draft PRs, bot authors (filtered upstream in fct)
-- Grain: one row per calendar month (Europe/Amsterdam timezone)

with base as (
    select
        pr_id,
        created_at,
        time_to_first_review_hours
    from {{ ref('fct_github__pull_requests') }}
    where
        created_at::date >= '{{ var("kpi_start_date") }}'::date
        -- filter only PRs with reviews
        and time_to_first_review_hours is not null
),

aggregated as (
    select
        date_trunc('month', created_at)::date as creation_month,
        count(*) as merged_pr_count,

        -- median hours from open to first human review
        median(time_to_first_review_hours) as median_time_to_first_review_hours,

        -- % of reviewed PRs where first review arrived within 24 hours
        sum(case when time_to_first_review_hours <= 24 then 1 else 0 end)
            / count(*) as pct_reviewed_within_24h

    from base
    group by 1
)

select * from aggregated
order by creation_month desc
