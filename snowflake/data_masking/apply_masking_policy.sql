{% macro apply_masking_policy(mp=[], meta_key="masking_policy") %}
    {{ return(adapter.dispatch('apply_masking_policy', 'sie_dbt_utils')(mp, meta_key)) }}
{% endmacro %}

{% macro default__apply_masking_policy(mp=[], meta_key="masking_policy") %}
{# -------------------------------------------------------------------------------------------------------------
Description:
    This macro can be used as a post_hook to apply masking policies to a model.
---------------------------------------------------------------------------------------------------------------- 
Parameters: 

    mp:        String/list of the masking policy model.     
    meta_key:  Optional to define the name of the meta_key in the yaml files.
---------------------------------------------------------------------------------------------------------------- #}

{# handle mp #}
{% if mp is string %}
    {% set mp_list=mp.split(',') %}
{% else %}
    {% set mp_list=mp %}
{% endif %}

{# create mp tupel list including mp_model ref to get dependency #}
{% set masking_policies = [] %}
{% for mp_model in mp_list %}
    {% do masking_policies.append((mp_model, render(ref(mp_model)))) %}
{% endfor %}

{% if execute %}

    {# map materialization #}
    {% set materialization_map = {"table": "table", "view": "view", "incremental": "table"} %}
    {% set materialization = materialization_map[model.config.get("materialized")] %}

    {# get model metadata #}
    {% set meta_columns = sie_dbt_utils.get_meta_objects(model.unique_id,meta_key) %}

    {# loop through columns #}
    {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
        {% set column = meta_tuple[0] %}
        {% set masking_policy_name = meta_tuple[1] %}
        {% if masking_policy_name is not none %}
            {# loop through attached masking policies #}
            {% for masking_policy in masking_policies %}
                {# if masking policy equals the one in the metadata, then apply masking #}
                {% if masking_policy[0]|lower == masking_policy_name|lower %}
                    {% set query %}
                        alter {{materialization}} {{this}} modify column {{column}} set masking policy {{masking_policy[1]}} force;
                    {% endset %}
                    {% do run_query(query) %}
                    {{ log(query|trim, info=False) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}

{% endif %}

{% endmacro %}
