{% macro get_matches(input_list, regex) %}

{% set results_list = [] %}
-- I'm not sure if there's a preferred syntax in SQL for variable naming, but I think it would be nice to replace
-- `l` with something more meaningful
{% for l in input_list %}
    {% if modules.re.match(regex, l, modules.re.IGNORECASE) %}
        {{ results_list.append(l) or "" }}
    {% endif %}
{% endfor %}

{{ return(results_list) }}

{% endmacro %}