WITH cross_border_segments AS (
  SELECT
    pipeline_segment_name
    , pipeline_name
    , properties_id
    , properties_id
    , first_country
    , second_country
    , pipeline_segment_length_km
    , geometry_coordinates
  FROM {{ ref('pipe_segments_stg') }}
  WHERE first_country != second_country
    AND first_country != 'XX'
    AND second_country != 'XX'
)

, first_country_intersection AS (
  SELECT
    country_code AS country
    , pipeline_segment_name
    , pipeline_name
    , pipeline_segment_length_km
    , extensions.ST_Length(
      extensions.ST_Intersection(geometry_coordinates, country_coordinates_polygon)
    ) AS cross_border_segment_length
    , extensions.ST_Length(
      extensions.ST_Intersection(geometry_coordinates, country_coordinates_polygon)
    )
    * pipeline_segment_length_km AS pipeline_segment_intersection_length_km
  FROM cross_border_segments
    INNER JOIN {{ ref('countries_stg') }} AS c
      ON c.country_code = first_country
)

, second_country_intersection AS (
  SELECT
    country_code AS country
    , pipeline_segment_name
    , pipeline_name
    , pipeline_segment_length_km
    , extensions.ST_Length(
      extensions.ST_Intersection(geometry_coordinates, country_coordinates_polygon)
    ) AS cross_border_segment_length
    , extensions.ST_Length(
      extensions.ST_Intersection(geometry_coordinates, country_coordinates_polygon)
    )
    * pipeline_segment_length_km AS pipeline_segment_intersection_length_km
  FROM cross_border_segments
    INNER JOIN {{ ref('countries_stg') }} AS c
      ON c.country_code = second_country
)

SELECT * FROM first_country_intersection
UNION ALL
SELECT * FROM second_country_intersection
