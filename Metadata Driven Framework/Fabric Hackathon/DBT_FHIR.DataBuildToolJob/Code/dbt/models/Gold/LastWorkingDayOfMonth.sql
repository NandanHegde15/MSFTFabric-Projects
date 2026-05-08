{{
    config(
        materialized = 'table',
        schema       = 'Gold'
    )
}}



SELECT [YearNumber],
			[MonthNumber],
			[LastWorkingDay] FROM {{ source('Silver','last_wrkday_mth') }} 
