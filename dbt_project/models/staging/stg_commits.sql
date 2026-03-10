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
        payload->'commit'->'author'->>'email'                   as author_email,

        payload->'committer'->>'login'                          as committer_login,
        (payload->'committer'->>'id')::bigint                   as committer_id,
        payload->'committer'->>'type'                           as committer_type,
        payload->'commit'->'committer'->>'email'                as committer_email,

        _extracted_at

    from source
),

final as (
    select
        *,
        {{ contributor_key('author_id', 'author_login', 'author_email') }}          as author_contributor_key,
        {{ contributor_key('committer_id', 'committer_login', 'committer_email') }} as committer_contributor_key
    from base
)

select * from final