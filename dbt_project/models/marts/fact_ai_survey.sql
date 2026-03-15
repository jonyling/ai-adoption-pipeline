-- FactAISurvey: core fact table with all measures and FK references
-- Includes 2 dbt-derived columns: net_jobs_change, ai_roi_index

with base as (
    select * from {{ ref('stg_ai_company_adoption') }}
),

dim_company as (
    select company_key, company_id from {{ ref('dim_company') }}
),

dim_date as (
    select date_key, survey_year, quarter from {{ ref('dim_date') }}
),

dim_ai_tool as (
    select ai_tool_key, ai_primary_tool from {{ ref('dim_ai_tool') }}
),

dim_usecase as (
    select usecase_key, ai_use_case from {{ ref('dim_ai_usecase') }}
),

dim_stage as (
    select stage_key, adoption_stage from {{ ref('dim_ai_adoption_stage') }}
),

dim_source as (
    select source_key, survey_source, data_collection_method
    from {{ ref('dim_survey_source') }}
)

select
    -- Surrogate primary key
    {{ dbt_utils.generate_surrogate_key(['base.response_id']) }} as survey_key,

    -- Natural key
    base.response_id,

    -- Foreign keys
    dc.company_key,
    dd.date_key,
    dt.ai_tool_key,
    du.usecase_key,
    ds.stage_key,
    dss.source_key,

    -- Measures
    base.revenue_growth_percent,
    base.cost_reduction_percent,
    base.productivity_change_percent,
    base.task_automation_rate,
    base.jobs_displaced,
    base.jobs_created,
    base.num_ai_tools_used,
    base.ai_projects_active,

    -- dbt derived columns
    base.jobs_created - base.jobs_displaced as net_jobs_change,

    case
        when base.ai_budget_percentage = 0 or base.ai_budget_percentage is null
            then null
        else round(
            (base.revenue_growth_percent + base.cost_reduction_percent)
            / base.ai_budget_percentage, 4
        )
    end as ai_roi_index

from base
left join dim_company  dc  on base.company_id         = dc.company_id
left join dim_date     dd  on base.survey_year         = dd.survey_year
                          and base.quarter             = dd.quarter
left join dim_ai_tool  dt  on base.ai_primary_tool     = dt.ai_primary_tool
left join dim_usecase  du  on base.ai_use_case         = du.ai_use_case
-- left join dim_stage    ds  on base.ai_adoption_stage   = ds.adoption_stage
left join dim_source   dss on base.survey_source       = dss.survey_source
                          and base.data_collection_method = dss.data_collection_method

 -- 6. CRITICAL FIX: Mapping Raw Adoption Stage to Standardized Dimension Labels
left join dim_stage    ds  on (
    case lower(base.ai_adoption_stage)
        when 'none'    then 'Exploring'
        when 'pilot'   then 'Piloting'
        when 'partial' then 'Scaling'
        when 'full'    then 'Transforming'
        else 'Unknown'
    end
) = ds.adoption_stage                         