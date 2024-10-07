SELECT
  properties__iso2 AS country_code
  , properties__name AS country_name
  , properties__area AS country_area
  , properties__pop2005 AS country_population_2005
  , CAST(geometry__coordinates AS extensions.GEOMETRY) AS country_coordinates_polygon
FROM pipe_segments.countries
