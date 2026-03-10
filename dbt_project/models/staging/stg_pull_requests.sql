with source as (
    select * from {{ source('raw_data', 'pull_requests') }}
),

base as (
    select 
        id::bigint                              as pr_id,
        (payload->>'number')::integer           as pr_number,
        payload->>'html_url'                    as pr_url,
        payload->>'state'                       as pr_state,
        payload->>'title'                       as pr_title,
        (payload->>'draft')::boolean            as is_draft,

        (payload->>'created_at')::timestamptz   as created_at,
        (payload->>'updated_at')::timestamptz   as updated_at,
        (payload->>'closed_at')::timestamptz    as closed_at,
        (payload->>'merged_at')::timestamptz    as merged_at,

        (payload->>'merged_at') is not null     as is_merged,
        payload->>'merge_commit_sha'            as merge_commit_sha,

        (payload->'user'->>'id')::bigint        as author_id,
        payload->'user'->>'login'               as author_login,
        payload->'user'->>'type'                as author_type,   -- user or bot
        payload->>'author_association'          as author_association,

        payload->'head'->>'ref'                 as source_branch,
        payload->'base'->>'ref'                 as target_branch,

        payload->'labels'                       as labels,

        (payload->>'commits')::integer          as commits_count,
        (payload->>'additions')::integer        as additions_count,
        (payload->>'deletions')::integer        as deletions_count,
        (payload->>'changed_files')::integer    as changed_files_count,
        (payload->>'comments')::integer         as comments_count,
        (payload->>'review_comments')::integer  as review_comments_count,
        json_array_length(payload->>'requested_reviewers') as requested_reviewer_count,

        _extracted_at

    from source
),

labels as (
    select
        pr_id,
        list(label.value->>'name') as label_names
    from base
    cross join json_each(labels) as label
    group by pr_id
),

final as (
    select 
        b.* exclude(labels), 
        coalesce(l.label_names, []) as label_names
    from base b
    left join labels l on b.pr_id = l.pr_id
)

select *
from final