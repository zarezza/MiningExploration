SELECT
    BOUNDARY_NAME,
    AGE_TYPE,
    COUNT(*) AS vertex_count,
    ROUND(AVG(DEPTH), 2) AS avg_depth,
    ROUND(STDDEV(DEPTH), 2) AS depth_variability,
    ROUND(AVG(LONGITUDE), 6) AS centroid_longitude,
    ROUND(AVG(LATITUDE), 6) AS centroid_latitude
FROM base_processed
WHERE DEPTH_CONFIDENCE IN ('H', 'M')
GROUP BY BOUNDARY_NAME, AGE_TYPE
HAVING vertex_count > 50
ORDER BY avg_depth DESC;