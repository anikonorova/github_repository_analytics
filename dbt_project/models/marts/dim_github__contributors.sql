with spine as (
    select 
        contributor_key,
        github_id,
        login,
        contributor_type,
        last_activity_at
    from {{ ref('int_github__contributors_spine') }}
),

pr_stats as (
    select
        author_contributor_key                  as contributor_key,
        count(*)                                as pull_request_count,
        min(created_at)                         as first_activity_at
    from {{ ref('stg_pull_requests') }}
    where author_contributor_key is not null
    group by author_contributor_key
),

review_stats as (
    select
        reviewer_contributor_key                as contributor_key,
        count(*)                                as review_count,
        min(submitted_at)                       as first_activity_at
    from {{ ref('stg_reviews') }}
    where reviewer_contributor_key is not null
    group by reviewer_contributor_key
),

issue_author_stats as (
    select
        author_contributor_key                  as contributor_key,
        count(*)                                as issue_count,
        min(created_at)                         as first_activity_at
    from {{ ref('stg_issues') }}
    where author_contributor_key is not null
    group by author_contributor_key
),

issue_assignee_stats as (
    select
        assignee_contributor_key                as contributor_key,
        count(*)                                as assigned_issue_count,
        min(created_at)                         as first_activity_at
    from {{ ref('stg_issues') }}
    where assignee_contributor_key is not null
    group by assignee_contributor_key
),

commit_author_stats as (
    select
        author_contributor_key                  as contributor_key,
        count(*)                                as commit_count,
        min(authored_at)                        as first_activity_at
    from {{ ref('stg_commits') }}
    where author_contributor_key is not null
    group by author_contributor_key
),

commit_committer_stats as (
    select
        committer_contributor_key               as contributor_key,
        count(*)                                as commits_committed_count,
        min(committed_at)                       as first_activity_at
    from {{ ref('stg_commits') }}
    where committer_contributor_key is not null
    group by committer_contributor_key
),

final as (
    select
        s.contributor_key,
        s.github_id,
        s.login,
        s.contributor_type,

        pr.pull_request_count is not null           as is_pr_author,
        ia.issue_count is not null                  as is_issue_author,
        ias.assigned_issue_count is not null        as is_assignee,
        rv.review_count is not null                 as is_reviewer,
        ca.commit_count is not null                 as is_commit_author,
        cc.commits_committed_count is not null      as is_committer,

        coalesce(pr.pull_request_count, 0)          as pull_request_count,
        coalesce(ia.issue_count, 0)                 as issue_count,
        coalesce(ias.assigned_issue_count, 0)       as assigned_issue_count,
        coalesce(rv.review_count, 0)                as review_count,
        coalesce(ca.commit_count, 0)                as commit_count,
        coalesce(cc.commits_committed_count, 0)     as commits_committed_count,
        
        least(
            pr.first_activity_at,
            ia.first_activity_at,
            ias.first_activity_at,
            rv.first_activity_at,
            ca.first_activity_at,
            cc.first_activity_at
        ) as first_activity_at,
        s.last_activity_at,

    from spine s
    left join pr_stats              pr  on s.contributor_key = pr.contributor_key
    left join review_stats          rv  on s.contributor_key = rv.contributor_key
    left join issue_author_stats    ia  on s.contributor_key = ia.contributor_key
    left join issue_assignee_stats  ias on s.contributor_key = ias.contributor_key
    left join commit_author_stats   ca  on s.contributor_key = ca.contributor_key
    left join commit_committer_stats cc on s.contributor_key = cc.contributor_key
)

select * from final