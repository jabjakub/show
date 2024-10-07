SELECT
  pipeline_segment_name
  , pipeline_name
  , first_country AS country
  , geometry_coordinates
  , pipeline_segment_length_km
FROM {{ ref('pipe_segments_stg') }}
WHERE first_country = second_country
