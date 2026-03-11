-- Grain: one row per PR
-- Excludes: draft PRs, bot authors

with pull_requests as (
    select *
    from {{ ref('stg_pull_requests') }}
    where is_draft = false
    and author_type = 'User'
),

reviews as (
    select 
        pr_number,
        {{ to_amsterdam('submitted_at') }} as submitted_at,
        reviewer_type
    from {{ ref('stg_reviews') }}
),

review_flags as (
    select
        pr_number,
        min(case 
                when reviewer_type = 'User' then submitted_at
                else null
            end
        ) as first_user_review_at,
        min(case 
                when reviewer_type = 'Bot' then submitted_at
                else null
            end
        ) as first_bot_review_at,
        count(*) as total_review_count
    from reviews
    group by pr_number
),

joined as (
    select
        pr.pr_id,
        pr.pr_number,
        pr.pr_title,
        pr.pr_state,
        pr.source_branch,
        pr.target_branch,
        pr.author_contributor_key,
        pr.author_login,
        pr.author_type,
        pr.author_id,
        pr.comments_count,
        pr.is_merged,
        -- transforming dates in UTC timezone to Amsterdam / Europe timezone
        -- macros dbt_project/macros/convert_timezone.sql
        {{ to_amsterdam('pr.created_at') }} as created_at,
        {{ to_amsterdam('pr.updated_at') }} as updated_at,
        {{ to_amsterdam('pr.closed_at') }} as closed_at,
        {{ to_amsterdam('pr.merged_at') }} as merged_at, 
        pr.additions_count,
        pr.deletions_count,
        pr.changed_files_count,
        pr.additions_count + pr.deletions_count as total_changes,
        pr.review_comments_count,
        pr.comments_count,
        pr.commits_count,

        -- review data
        rf.first_user_review_at,
        rf.first_bot_review_at,
        rf.total_review_count,

        -- cycle time
        case
            when pr.is_merged
            then date_diff('hour', pr.created_at, pr.merged_at)
        end as cycle_time_hours,
        case
            when pr.is_merged
            then date_diff('day', pr.created_at, pr.merged_at)
        end as cycle_time_days,

        -- review turnaround in hours (open → first review)
        case
            when rf.first_user_review_at is not null
            then date_diff('hour', pr.created_at, rf.first_user_review_at)
        end as time_to_first_review_hours,

        pr._extracted_at

    from pull_requests pr
    left join review_flags rf
        on pr.pr_number = rf.pr_number
),

final as (
    select *,
    case
            when total_changes <= 50     then 'small'
            when total_changes <= 250    then 'medium'
            else                              'large'
        end as pr_size
    from joined
)

select * from final