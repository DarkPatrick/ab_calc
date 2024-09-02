with members as (
    select
        experiments.variation[indexOf(experiments.id, {exp_id})] as variation,
        unified_id,
        argMinIf(session_id, datetime, session_id > 0) as session_id,
        min(toUnixTimestamp(datetime)) as exp_start_dt,
        argMin(rights, datetime) as rights,
        argMin(country, datetime) as country,
        argMin(source, datetime) as source,
        argMin(multiIf(os in ('ios', 'os x'), 'ios', os), datetime) as os
    from
        default.ug_rt_events_web
    where
        date = '{date}'
    and
        datetime between toDateTime({datetime_start}) and toDateTime({datetime_end})
    and
        has(experiments.id, {exp_id})
    and
        unified_id > 0
    and (
        '{custom_confirm_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = '{exp_id}'
        or '{custom_confirm_event}' <> 'App Experiment Start' and event = '{custom_confirm_event}'
    )
    and
        multiIf(
            '{platform}' = 'Desktop',  platform = 1,
            '{platform}' = 'Phone', platform = 2, 
            '{platform}' = 'Tablet', platform = 3, 
            '{platform}' = 'Mobile', platform > 1, 
            1
        )
    and
        if('{custom_confirm_event}' = 'Landing Upgrade Open', value not like 'email%', 1)
    and
        ('{custom_confirm_include_values}' = '' or value in ('{custom_confirm_include_values}'))
    and
        ('{custom_confirm_exclude_values}' = '' or value not in ('{custom_confirm_exclude_values}'))
    group by
        variation,
        unified_id
    having
        session_id > 0
    union all select
        experiments.variation[indexOf(experiments.id, {exp_id})] as variation,
        unified_id,
        argMinIf(session_id, datetime, session_id > 0) as session_id,
        min(toUnixTimestamp(datetime)) AS exp_start_dt,
        argMin(rights,datetime) AS rights,
        argMin(country,datetime) AS country,
        argMin(source, datetime) as source,
        argMin(os, datetime) as os
    from
        default.ug_rt_events_app
    where
        date = '{date}'
    and
        datetime between toDateTime({datetime_start}) and toDateTime({datetime_end})
    and
        has(experiments.id, {exp_id})
    and
        unified_id > 0
    and
        payment_account_id > 0
    and (
        '{custom_confirm_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = '{exp_id}'
        or '{custom_confirm_event}' <> 'App Experiment Start' and event = '{custom_confirm_event}'
    )
    and
        ('{custom_confirm_include_values}' = '' or value in ('{custom_confirm_include_values}'))
    and
        ('{custom_confirm_exclude_values}' = '' or value not in ('{custom_confirm_exclude_values}'))
    and
        multiIf(
            '{platform}' = 'Desktop',  platform = 1,
            '{platform}' = 'Phone', platform = 2, 
            '{platform}' = 'Tablet', platform = 3, 
            '{platform}' = 'Mobile', platform > 1, 
            1
        )
    group by
        variation,
        unified_id
    having
        session_id > 0
    and
        ('{source}' = 'All' or '{source}' = source)
)

select
    *
from
    members
where
    {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
and
    multiIf(
        '{country}' in ('US', 'CA', 'GB', 'AU'), country = '{country}', 
        '{country}' = 'Europe', country in ('BY', 'BG', 'HU', 'XK', 'MD', 'PL', 'RU', 'RO', 'SK', 'UA', 'CZ', 'AT', 'BE', 'DE', 'LI', 'LU', 'MC', 'NL', 'FR', 'CH', 'AX', 'DK', 'IE', 'IS', 'LV', 'LT', 'NO', 'FI', 'SE', 'AL', 'AD', 'BA', 'VA', 'GR', 'ES', 'IT', 'MK', 'MT', 'PT', 'SM', 'RS', 'SI', 'HR', 'ME'), 
        '{country}' = 'Asia', country in ('JP', 'KR', 'PH', 'TR', 'TH', 'SG', 'MY', 'KZ', 'ID', 'VN', 'IN'), 
        '{country}' = 'Latam', country in ('AR', 'BO', 'BR', 'VE', 'HT', 'GP', 'GT', 'HN', 'DO', 'CO', 'CR', 'CU', 'MQ', 'MX', 'NI', 'PA', 'PY', 'PE', 'PR', 'SV', 'BL', 'MF', 'UY', 'GF', 'CL', 'EC'), 
        1
    )
and
    ({custom_sql})
