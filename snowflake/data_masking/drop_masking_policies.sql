{% macro drop_masking_policies(meta_key="masking_policy", drop_mp=true, mp_schema=generate_schema_name('CUSTOM')|trim, mp_name=none) %}
    {{ return(adapter.dispatch('drop_masking_policies', 'sie_dbt_utils')(meta_key, drop_mp, mp_schema, mp_name)) }}
{% endmacro %}

{% macro default__drop_masking_policies(
        meta_key="masking_policy",
        drop_mp=true,
        mp_schema=generate_schema_name('CUSTOM')|trim,
        mp_name=none
) %}

{# -------------------------------------------------------------------------------------------------------------
Description:
    This macro can be used as a run-operation to unapply and drop all masking policies.
---------------------------------------------------------------------------------------------------------------- 
Parameters: 

    meta_key:    Optional to define the name of the meta_key in the yaml files.
    drop_mp:     Optional to disable the drop of the masking policies and only unapply them on the columns.
    mp_schema:   Optional to change the schema, where the masking policies are stored.
    mp_name:     Optional to select a single masking_policy to drop/unapply.
---------------------------------------------------------------------------------------------------------------- #}
{% if execute %}

    {{ log('\n' ~
        'meta_key: ' ~ meta_key ~ '\n' ~
        'drop_mp: ' ~ drop_mp ~ '\n' ~
        'mp_schema: ' ~ mp_schema ~ '\n' ~
        'mp_name: ' ~ mp_name
    , info=True) }}

    {# unapply masking policies #}
    {% for node in graph.nodes.values() -%}

        {% set database = node.database | string %}
        {% set schema   = node.schema | string %}
        {% set alias = node.alias %}

        {# map materialization #}
        {% set materialization_map = {"table": "table", "view": "view", "incremental": "table"} %}
        {% set materialization = materialization_map[node.config.get("materialized")] %}

        {% set meta_columns = sie_dbt_utils.get_meta_objects(node.unique_id,meta_key) %}

        {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
            {% set column = meta_tuple[0] %}
            {% set masking_policy_name = meta_tuple[1] %}
            {% if masking_policy_name is not none %}
                {% set query %}
                    alter {{materialization}} {{database}}.{{schema}}.{{alias}} modify column {{column}} unset masking policy
                {% endset %}
                {% do run_query(query) %}
                {{ log(query|trim, info=True) }}
            {% endif %}
        {% endfor %}

    {% endfor %}

    {# drop masking policies #}
    {% if drop_mp is true %}

        {% set masking_policy_list_sql %}     
            show masking policies in {{target.database}}.{{mp_schema}};
            select $3||'.'||$4||'.'||$2 as masking_policy from table(result_scan(last_query_id()))
            {% if mp_name is not none %} where $2 = '{{mp_name|upper}}' {% endif %};
        {% endset %}

        {% set masking_policy_list = dbt_utils.get_query_results_as_dict(masking_policy_list_sql) %}

        {% for masking_policy_in_db in masking_policy_list['MASKING_POLICY'] %}
            {% set query %}
                drop masking policy {{masking_policy_in_db}}
            {% endset %}
            {% do run_query(query) %}
            {{ log(query|trim, info=True) }}
        {% endfor %}
    {% endif %}

{% endif %}

{% endmacro %}