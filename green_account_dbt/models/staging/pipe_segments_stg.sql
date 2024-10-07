SELECT *
FROM (
  SELECT
    type
    , geometry__type AS geometry_type
    , geometry__coordinates AS geometry_coordinates_raw
    , CAST(
      CAST(geometry__coordinates AS extensions.GEOMETRY) AS extensions.GEOMETRY
    ) AS geometry_coordinates
    , properties__name AS pipeline_segment_name
    , REGEXP_REPLACE(properties__name, '_Seg_\d+', '') AS pipeline_name
    , properties__id AS properties_id
    , REPLACE(CAST (properties__country_code[0] AS VARCHAR), '"', '') AS first_country
    , REPLACE(CAST (properties__country_code[1] AS VARCHAR), '"', '') AS second_country
    , properties__param__length_km AS pipeline_segment_length_km
    , _dlt_id
    , ROW_NUMBER()
      OVER (
        PARTITION BY
          geometry__coordinates, properties__name, properties__param__length_km
      )
    AS rn
  FROM pipe_segments.pipe_segments
)
WHERE rn = 1
