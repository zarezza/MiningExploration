WITH SurveySummary AS (
    SELECT
        SURVEY_NAME,
        VERTEX,
        CAST(AVG(DEPTH) AS DECIMAL(10, 3)) AS Avg_Depth,
        MAX(ELEVATION) AS Max_Elevation
    FROM
        survey_data_view
    GROUP BY
        SURVEY_NAME, VERTEX
)
SELECT
    SURVEY_NAME,
    VERTEX,
    Avg_Depth,
    Max_Elevation,
    ROW_NUMBER() OVER (PARTITION BY SURVEY_NAME ORDER BY Avg_Depth DESC) AS Depth_Rank
FROM
    SurveySummary
ORDER BY
    SURVEY_NAME, Depth_Rank