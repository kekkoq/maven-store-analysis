WITH lift_test_data AS (
    -- CTE 1: Calculates Revenue Per Session (RPS) for the two test pages
    -- NOTE: This requires joining website_pageviews (wp), website_sessions (ws, implicit), and orders (od).
    SELECT 
        wp.pageview_url AS pageview_url,
        COUNT(DISTINCT wp.website_session_id) AS unique_sessions,
        SUM(od.price_usd) AS total_revenue,
        SUM(od.price_usd) * 1.0 / COUNT(DISTINCT wp.website_session_id) AS rev_per_session
    FROM website_pageviews AS wp
    LEFT JOIN orders AS od
        ON wp.website_session_id = od.website_session_id
    WHERE 
        wp.pageview_url IN ('/billing', '/billing-2') 
        AND wp.created_at >= '2012-09-10' 
        AND wp.created_at < '2012-11-10'
    GROUP BY 
        wp.pageview_url
),
incremental_lift AS (
    -- CTE 2: Calculates the lift (delta) in RPS
    SELECT
        -- Use the correct metric (rev_per_session) for calculating lift
        MAX(CASE WHEN pageview_url = '/billing-2' THEN rev_per_session END) -
        MAX(CASE WHEN pageview_url = '/billing' THEN rev_per_session END) AS delta_rev_per_session
    FROM lift_test_data
),
total_sessions_projection AS (
    -- CTE 3: Calculates the number of sessions to project the lift onto
    -- We only need to count sessions where the user saw the billing page (either version)
    SELECT
        COUNT(DISTINCT website_session_id) AS total_sessions_to_project
    FROM website_pageviews 
    WHERE 
        created_at BETWEEN '2012-11-10' AND '2012-11-27' -- Start after test end date
        AND pageview_url IN ('/billing', '/billing-2')
)
-- Final SELECT: Calculate the projected incremental sales
SELECT 
    ROUND(tsp.total_sessions_to_project * il.delta_rev_per_session, 2) AS projected_incremental_revenue
FROM total_sessions_projection AS tsp
CROSS JOIN incremental_lift AS il;