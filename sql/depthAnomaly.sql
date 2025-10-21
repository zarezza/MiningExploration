WITH grid_summary AS (
    SELECT 
        grid_x, 
        grid_y,
        COUNT(*) AS cell_points,
        CAST(AVG(DEPTH) AS DECIMAL(10, 3)) AS cell_avg_depth,
        CAST(STDDEV(DEPTH) AS DECIMAL(10, 3)) AS cell_std_depth
    FROM 
        survey_data_view
    WHERE 
        grid_x IS NOT NULL 
        AND grid_y IS NOT NULL 
        AND DEPTH IS NOT NULL
    GROUP BY 
        grid_x, grid_y
),
anomaly_calc AS (
    SELECT 
        s.VERTEX, 
        s.SURVEY_LINE, 
        CAST(s.LONGITUDE AS DECIMAL(10, 6)) AS longitude,
        CAST(s.LATITUDE AS DECIMAL(10, 6)) AS latitude,
        CAST(s.DEPTH AS DECIMAL(10, 3)) AS depth, 
        g.cell_avg_depth, 
        g.cell_std_depth,
        g.cell_points,
        s.grid_x,
        s.grid_y,
        CAST(s.DEPTH - g.cell_avg_depth AS DECIMAL(10, 3)) AS depth_anomaly
    FROM 
        survey_data_view s
    JOIN 
        grid_summary g ON s.grid_x = g.grid_x AND s.grid_y = g.grid_y
    WHERE 
        s.VERTEX IS NOT NULL 
        AND s.SURVEY_LINE IS NOT NULL
        AND s.DEPTH IS NOT NULL
        AND s.LONGITUDE IS NOT NULL
        AND s.LATITUDE IS NOT NULL
)
SELECT *
FROM anomaly_calc
WHERE ABS(depth_anomaly) > 50
ORDER BY 
    depth_anomaly DESC