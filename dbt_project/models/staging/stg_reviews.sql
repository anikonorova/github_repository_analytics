with source as (
    select * from {{ source('raw_data', 'reviews') }}
),

base as (
    select
        id::bigint                                  as review_id,
        pr_number::integer                          as pr_number,
        payload->>'commit_id'                       as commit_id,
        payload->>'state'                           as review_state,
        payload->>'html_url'                        as review_url,
        (payload->>'submitted_at')::timestamptz     as submitted_at,
        payload->'user'->>'login'                   as reviewer_login,
        (payload->'user'->>'id')::bigint            as reviewer_id,
        payload->'user'->>'type'                    as reviewer_type,
        payload->'user'->>'email'                   as reviewer_email,
        payload->>'author_association'              as author_association,
        _extracted_at
    from source
),

final as (
    select
        *,
        {{ contributor_key('reviewer_id', 'reviewer_login', 'reviewer_email') }} as reviewer_contributor_key
    from base
)

select * from final
