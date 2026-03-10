with pull_request_authors as (
    select
        author_contributor_key      as contributor_key,
        author_type                 as contributor_type,
        author_id                   as github_id,
        author_login                as login,
        created_at                  as activity_at
    from {{ ref('stg_pull_requests') }}
),

reviewers as (
    select
        reviewer_contributor_key    as contributor_key,
        reviewer_type               as contributor_type,
        reviewer_id                 as github_id,
        reviewer_login              as login,
        submitted_at                as activity_at
    from {{ ref('stg_reviews') }}
),

issue_authors as (
    select
        author_contributor_key      as contributor_key,
        author_type                 as contributor_type,
        author_id                   as github_id,
        author_login                as login,
        created_at                  as activity_at
    from {{ ref('stg_issues') }}
),

issue_assignees as (
    select
        assignee_contributor_key    as contributor_key,
        assignee_type               as contributor_type,
        assignee_id                 as github_id,
        assignee_login              as login,
        created_at                  as activity_at
    from {{ ref('stg_issues') }}
    -- assignees are present in rare cases in the dataset, so filter out issues without them.
    where contributor_key is not null
),

commit_authors as (
    select
        author_contributor_key      as contributor_key,
        author_type                 as contributor_type,
        author_id                   as github_id,
        author_login                as login,
        authored_at                 as activity_at
    from {{ ref('stg_commits') }}
),

commit_committers as (
    select
        committer_contributor_key   as contributor_key,
        committer_type              as contributor_type,
        committer_id                as github_id,
        committer_login             as login,
        committed_at                as activity_at
    from {{ ref('stg_commits') }}
),

all_contributor_events as (
    select * from pull_request_authors
    union all
    select * from reviewers
    union all
    select * from issue_authors
    union all
    select * from issue_assignees
    union all
    select * from commit_authors
    union all
    select * from commit_committers
),

deduplicated as (
    select
        contributor_key,
        contributor_type,
        github_id,
        login,
        activity_at as last_activity_at,
        row_number() over (partition by contributor_key order by activity_at desc) as rn
    from all_contributor_events
    where contributor_key is not null
    qualify rn = 1
)

select * exclude(rn) from deduplicated