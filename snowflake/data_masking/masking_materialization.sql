{%- materialization masking_policy, adapter='snowflake' -%}

  {%- set original_query_tag = set_query_tag() -%}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database) -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}
    create masking policy if not exists {{ target_relation }} as 
      {{ sql }}
    ;
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}
  {% do unset_query_tag(original_query_tag) %}

  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

