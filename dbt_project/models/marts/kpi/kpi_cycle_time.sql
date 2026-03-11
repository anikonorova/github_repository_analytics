-- KPI: PR Cycle Time
-- Definition: duration in hours from PR opened to merged
-- Excludes: draft PRs, bot authors, negative/zero cycle times
-- Grain: one row per calendar month (Europe/Amsterdam)

with base as (
    select
        pr_id,
        created_at,
        merged_at,
        cycle_time_hours,
        pr_size
    from {{ ref('fct_github__pull_requests') }}
    where
        is_merged = true
        and cycle_time_hours > 0
        and created_at::date >= '{{ var("kpi_start_date") }}'::date
),

aggregated as (
    select
        date_trunc('month', created_at)::date as creation_month,
        pr_size,
        count(*) as merged_pr_count,

        -- exclude large PRs: insufficient monthly sample size (<20/month)
        -- for statistical reliability of median calculation
        (case 
            when pr_size != 'large' then median(cycle_time_hours)
            else null  
        end) as median_cycle_time_hours,
        avg(cycle_time_hours) as mean_cycle_time_hours

    from base
    group by 1,2
)

select * from aggregated
order by 
    creation_month desc,
    case pr_size
        when 'small'    then 1
        when 'medium'   then 2
        when 'large'    then 3
    end