select *
from {{ ref('stg_pull_requests') }}
where merged_at is not null
  and merged_at < created_at