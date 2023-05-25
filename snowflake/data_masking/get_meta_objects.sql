{% macro get_meta_objects(node_unique_id, meta_key) %}
    {{ return(adapter.dispatch('get_meta_objects', 'sie_dbt_utils')(node_unique_id, meta_key)) }}
{% endmacro %}

{% macro default__get_meta_objects(node_unique_id, meta_key) %}
{# get the metadata of the node #}
	{% if execute %}

        {% set meta_columns = [] %}
        {% set columns = graph.nodes[node_unique_id]['columns']  %}
        
        {% if meta_key is not none %}
            {% for column in columns if graph.nodes[node_unique_id]['columns'][column]['meta'][meta_key] | length > 0 %}
                {% set meta_dict = graph.nodes[node_unique_id]['columns'][column]['meta'] %}
                {% for key, value in meta_dict.items() if key == meta_key %}
                    {% set meta_tuple = (column ,value ) %}
                    {% do meta_columns.append(meta_tuple) %}
                {% endfor %}
            {% endfor %}
        {% else %}
            {% do meta_columns.append(column|upper) %}
        {% endif %}

        {{ return(meta_columns) }}

    {% endif %}
{% endmacro %}