WITH regional_bins AS (
  SELECT
    CAST(EASTING / 50000 AS INT) AS region_x,
    CAST(NORTHING / 50000 AS INT) AS region_y,
    LATITUDE,
    LONGITUDE,
    DEPTH,
    DEPTH_CONFIDENCE
  FROM base_processed
  WHERE DEPTH IS NOT NULL
)
SELECT
  region_x,
  region_y,
  COUNT(*) AS n_points,
  ROUND(AVG(DEPTH), 2) AS avg_depth,
  SUM(CASE WHEN DEPTH_CONFIDENCE = 'H' THEN 1 ELSE 0 END) AS high_conf_points,
  ROUND(AVG(LONGITUDE), 6) AS centroid_longitude,
  ROUND(AVG(LATITUDE), 6) AS centroid_latitude
FROM regional_bins
GROUP BY region_x, region_y
HAVING n_points > 10
ORDER BY n_points DESC;