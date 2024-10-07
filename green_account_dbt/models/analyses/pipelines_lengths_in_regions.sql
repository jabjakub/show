WITH pipelines AS (
  SELECT
    country
    , pipeline_segment_name
    , pipeline_name
    , pipeline_segment_length_km
  FROM {{ ref('segments_in_countries') }}
  UNION ALL
  SELECT
    country
    , pipeline_segment_name
    , pipeline_name
    , pipeline_segment_intersection_length_km
  FROM {{ ref('cross_country_segments') }}

)

SELECT
  country
  , SUM(pipeline_segment_length_km) AS country_pipeline_total_length
FROM pipelines
GROUP BY 1
