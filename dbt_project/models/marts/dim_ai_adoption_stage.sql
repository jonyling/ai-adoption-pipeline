-- DimAIAdoptionStage: Mapping raw values to standardized professional stages
with unique_stages as (
    select distinct ai_adoption_stage
    from {{ ref('stg_ai_company_adoption') }}
    where ai_adoption_stage is not null
)

select
    -- 1. Generate surrogate key based on the raw source value
    {{ dbt_utils.generate_surrogate_key(['ai_adoption_stage']) }} as stage_key,
    
    -- 2. Standardized Stage Name (Matches your schema.yml accepted_values)
    case lower(ai_adoption_stage)
        when 'none'    then 'Exploring'
        when 'pilot'   then 'Piloting'
        when 'partial' then 'Scaling'
        when 'full'    then 'Transforming'
        else 'Unknown'
    end as adoption_stage,

    -- 3. Numerical order for sorting in BI tools (1-4)
    case lower(ai_adoption_stage)
        when 'none'    then 1
        when 'pilot'   then 2
        when 'partial' then 3
        when 'full'    then 4
        else 5
    end as stage_order,

    -- 4. Professional labels for Display (e.g., "Stage 1: Exploring")
    case lower(ai_adoption_stage)
        when 'none'    then 'Stage 1: Exploring'
        when 'pilot'   then 'Stage 2: Piloting'
        when 'partial' then 'Stage 3: Scaling'
        when 'full'    then 'Stage 4: Transforming'
        else 'Stage 5: Unknown'
    end as stage_label

from unique_stages

