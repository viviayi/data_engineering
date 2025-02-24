WITH quarterly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS total_revenue
    FROM {{ ref("fact_trips") }}
    WHERE total_amount IS NOT NULL
    GROUP BY 1, 2, 3
),

revenue_with_prev_year AS (
    SELECT
        year,
        quarter,
        service_type,
        total_revenue,
        -- 获取前一年同季度的收入
        LAG(total_revenue) OVER (
            PARTITION BY service_type, quarter
            ORDER BY year
        ) AS prev_year_revenue
    FROM quarterly_revenue
),

revenue_with_growth AS (
    SELECT
        year,
        quarter,
        service_type,
        total_revenue,
        prev_year_revenue,
        -- 计算 YoY 增长率
        ROUND(
            (total_revenue - COALESCE(prev_year_revenue, 0)) 
            / NULLIF(COALESCE(prev_year_revenue, 1), 0) * 100, 2
        ) AS yoy_growth
    FROM revenue_with_prev_year
)

SELECT * FROM revenue_with_growth
ORDER BY service_type, year, quarter