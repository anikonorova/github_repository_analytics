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
        reviewer_type,
        reviewer_contributor_key
    from {{ ref('stg_reviews') }}
),

review_flags as (
    select
        r.pr_number,
        min(case 
                when r.reviewer_type = 'User' then r.submitted_at
                else null
            end
        ) as first_user_review_at,
        min(case 
                when r.reviewer_type = 'Bot' then r.submitted_at
                else null
            end
        ) as first_bot_review_at,
        sum(case 
                when r.reviewer_contributor_key = pr.author_contributor_key then 1
                else 0
            end
        ) as self_review_count,
        count(*) as total_review_count
    from reviews r
        join pull_requests pr on r.pr_number = pr.pr_number
    group by r.pr_number
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
        rf.self_review_count,
        rf.total_review_count,

        -- cycle time in hours (open → merge)
        case
            when pr.is_merged
            then date_diff('hour', pr.created_at, pr.merged_at)
        end as cycle_time_hours,

        -- review turnaround in hours (open → first review)
        case
            when rf.first_user_review_at is not null
            then date_diff('hour', pr.created_at, rf.first_user_review_at)
        end as time_to_first_review_hours,

        pr._extracted_at

    from pull_requests pr
    left join review_flags rf
        on pr.pr_number = rf.pr_number
)

select * from joined