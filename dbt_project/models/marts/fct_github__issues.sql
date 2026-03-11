-- Grain: one row per issue
-- Excludes: bot authors

with issues as (
    select * from {{ ref('stg_issues') }}
    where author_type = 'User'
),

enriched as (
    select
        issue_id,
        issue_number,
        issue_title,
        issue_state,
        state_reason,
        {{ to_amsterdam('created_at') }} as created_at,
        {{ to_amsterdam('updated_at') }} as updated_at,
        {{ to_amsterdam('closed_at') }} as closed_at,
        author_contributor_key,
        author_login,
        author_id,
        author_type,
        author_association,
        comment_count,
        label_names,

        list_contains(label_names, 'Bug')  as is_bug,

        case
            when closed_at is not null
            then date_diff('hour', created_at, closed_at)
        end as hours_to_close,

        case
            when closed_at is not null
            then datediff('day', created_at, closed_at)
        end as days_to_close,

        (closed_at is not null) as is_resolved,
        (issue_state = 'closed' and state_reason = 'completed') as is_completed,

        -- resolution time buckets
        case
            when closed_at is null                                 then 'open'
            when datediff('day', created_at, closed_at) <= 7       then 'within_7_days'
            when datediff('day', created_at, closed_at) <= 14      then 'within_14_days'
            when datediff('day', created_at, closed_at) <= 30      then 'within_30_days'
            else                                                        'over_30_days'
        end as resolution_bucket,

        _extracted_at
    from issues
)

select * from enriched