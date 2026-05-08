{{
    config(
        materialized = 'table',
        schema       = 'Silver'
    )
}}

SELECT [patient_sk],
			[patient_id],
			[name_family],
			[name_given],
			[full_name],
			[gender],
			[birth_date],
			[address_use],
			[address_line],
			[address_city],
			[address_state],
			[address_postalcode],
			[address_text],
			[telecom_system],
			[telecom_use],
			[telecom_value],
			{{ to_datetime2('effective_start_date' )}} AS [effective_start_date],
			{{ to_datetime2('effective_end_date' )}} AS [effective_end_date],
			[is_current],
			{{ to_datetime2('meta_lastupdated' )}} AS [meta_lastupdated],
			[meta_source],
			[meta_versionid],
			[resource_type],
			[search_mode],
			[full_url],
			{{ to_datetime2('Silver_loaded_at' )}} AS [Silver_loaded_at]
FROM {{ source('Silver', 'dim_patient') }} where [is_current] = 1