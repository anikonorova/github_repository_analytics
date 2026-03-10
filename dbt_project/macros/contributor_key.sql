{#
    This macro generates a surrogate key for a GitHub contributor.
    GitHub ID and login fields may be null; in such cases, the email field may be present.

    Using 'login_' and 'email_' as prefixes preventing ambiguity (e.g., login = '12345' vs. github_id = '12345')
#}
{% macro contributor_key(id_col, login_col, email_col) %}
    case
        when {{ id_col }} is not null   then md5({{ id_col }}::varchar)
        when {{ login_col }} is not null then md5('login_' || {{ login_col }})
        when {{ email_col }} is not null then md5('email_' || {{ email_col }})
        else null
    end
{% endmacro %}
