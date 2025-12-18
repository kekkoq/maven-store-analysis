-- CTE 1: Calculates the conversion rate for the A/B test (lander vs. home)
WITH conversion_test_analysis AS (
    SELECT
        wp.pageview_url AS landing_page,
        COUNT(DISTINCT ws.website_session_id) AS test_sessions,
        COUNT(DISTINCT od.order_id) AS test_orders,
        (COUNT(DISTINCT od.order_id) * 1.0) / COUNT(DISTINCT ws.website_session_id) AS conversion_rate
    FROM website_sessions AS ws
    INNER JOIN website_pageviews AS wp
        ON ws.website_session_id = wp.website_session_id
        -- Ensure this is the FIRST pageview in the session
        AND wp.website_pageview_id = (
            SELECT MIN(wp_sub.website_pageview_id)
            FROM website_pageviews AS wp_sub
            WHERE wp_sub.website_session_id = ws.website_session_id
        )
    LEFT JOIN orders AS od
        ON ws.website_session_id = od.website_session_id
    WHERE 
        ws.created_at < '2012-07-28' -- End date for the experiment
        AND wp.website_pageview_id >= 23504 -- Start pageview ID (can be simplified if '2012-06-19' is the actual start date)
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
        AND wp.pageview_url IN ('/lander-1', '/home') -- Only for the test pages
    GROUP BY 1
),

-- CTE 2: Finds the incremental lift (delta) between the two conversion rates
-- The primary reason for using MAX (or MIN) in this conditional aggregation is to handle the NULL values created by the CASE statements, ensuring that the single non-null conversion rate is returned.
incremental_rate AS (
    SELECT
        MAX(CASE WHEN landing_page = '/lander-1' THEN conversion_rate END) -
        MAX(CASE WHEN landing_page = '/home' THEN conversion_rate END) AS delta_conversion
    FROM conversion_test_analysis
),

-- CTE 3: Calculates the total projected sessions for the future period
total_sessions_projection AS (
    SELECT
        COUNT(DISTINCT ws.website_session_id) AS total_sessions
    FROM website_sessions AS ws
    WHERE 
        ws.created_at BETWEEN '2012-07-28' AND '2012-11-27' -- Sessions *after* the test, up to the end date
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
        -- No need for the session_cutoff logic if the start date of the projection period is '2012-07-28'
)

-- Final SELECT: Calculate the projected sales
SELECT 
    tsp.total_sessions * ir.delta_conversion AS projected_incremental_sales
FROM total_sessions_projection AS tsp
CROSS JOIN incremental_rate AS ir;