with source as (
    select * from {{ source('raw_data', 'commits') }}
),

base as (
    select
        payload,
        sha                                                     as commit_sha,
        payload->>'html_url'                                    as commit_url,
        payload->'commit'->>'message'                           as commit_message,

        (payload->'commit'->'author'->>'date')::timestamptz     as authored_at,
        (payload->'commit'->'committer'->>'date')::timestamptz  as committed_at,

        payload->'author'->>'login'                             as author_login,
        (payload->'author'->>'id')::bigint                      as author_id,
        payload->'author'->>'type'                              as author_type,

        payload->'committer'->>'login'                          as committer_login,
        payload->'committer'->>'id'                             as committer_id,
        payload->'committer'->>'type'                           as committer_type,

        _extracted_at
    
    from source
)

-- add deduplication 

select * from base