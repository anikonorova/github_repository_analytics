with source as (
    select * from {{ source('raw_data', 'issues') }}
),

base as (
    select
        id::bigint                                          as issue_id,
        (payload->>'number')::integer                       as issue_number,
        payload->>'title'                                   as issue_title,
        payload->>'state'                                   as issue_state,
        payload->>'state_reason'                            as state_reason,
        payload->>'html_url'                                as issue_url,

        (payload->>'created_at')::timestamptz               as created_at,
        (payload->>'updated_at')::timestamptz               as updated_at,
        (payload->>'closed_at')::timestamptz                as closed_at,

        payload->'user'->>'login'                           as author_login,
        (payload->'user'->>'id')::bigint                    as author_id,
        payload->'user'->>'type'                            as author_type,
        payload->>'author_association'                      as author_association,

        payload->'assignee'->>'login'                       as assignee_login,
        (payload->'assignee'->>'id')::bigint                as assignee_id,
        payload->'assignee'->>'type'                        as assignee_type,

        (payload->>'comments')::integer                     as comment_count,

        (payload->'reactions'->>'total_count')::integer     as reaction_total,
        (payload->'reactions'->>'+1')::integer              as reaction_thumbs_up,
        (payload->'reactions'->>'-1')::integer              as reaction_thumbs_down,
        (payload->'reactions'->>'laugh')::integer           as reaction_laugh,
        (payload->'reactions'->>'hooray')::integer          as reaction_hooray,
        (payload->'reactions'->>'confused')::integer        as reaction_confused,
        (payload->'reactions'->>'heart')::integer           as reaction_heart,
        (payload->'reactions'->>'rocket')::integer          as reaction_rocket,
        (payload->'reactions'->>'eyes')::integer            as reaction_eyes,

        payload->'labels'                                   as labels,

        _extracted_at

    from source
),

labels as (
    select
        issue_id,
        list(label.value->>'name') as label_names
    from base
    cross join json_each(labels) as label
    group by issue_id
),

final as (
    select 
        b.* exclude(labels), 
        coalesce(l.label_names, []) as label_names
    from base b
    left join labels l on b.issue_id = l.issue_id
)

select *
from final