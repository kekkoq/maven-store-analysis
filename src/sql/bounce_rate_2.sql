WITH session_first_pageview AS (
    -- Identify the first pageview ID for relevant sessions
    SELECT
        wp.website_session_id AS session_id,
        MIN(wp.website_pageview_id) AS first_pageview_id,
        -- Use a window function to find the total pageview count for the session.
        COUNT(wp.website_pageview_id) OVER (PARTITION BY wp.website_session_id) AS total_pageviews
    FROM website_pageviews AS wp
    INNER JOIN website_sessions AS ws
        ON wp.website_session_id = ws.website_session_id
    WHERE 
        ws.created_at < '2012-07-28'
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
        AND wp.website_pageview_id > 23504
    GROUP BY 
        wp.website_session_id, 
        wp.website_pageview_id -- Group by pageview_id to ensure the window function works correctly across all rows
),
session_details AS (
    -- Get the landing page URL and determine bounce status in one step
    SELECT
        sfp.session_id,
        wp.pageview_url AS landing_page,
        -- Use a CASE statement to assign 1 if it was a bounce (total_pageviews = 1), otherwise 0
        CASE WHEN sfp.total_pageviews = 1 THEN 1 ELSE 0 END AS is_bounced
    FROM session_first_pageview AS sfp
    INNER JOIN website_pageviews AS wp
        ON sfp.first_pageview_id = wp.website_pageview_id
    WHERE wp.pageview_url IN ('/home', '/lander-1')
)
SELECT
    sd.landing_page,
    COUNT(sd.session_id) AS total_sessions,
    SUM(sd.is_bounced) AS bounced_sessions,
    ROUND(SUM(sd.is_bounced) * 1.0 / COUNT(sd.session_id), 4) AS bounce_rate
FROM session_details AS sd
GROUP BY sd.landing_page
ORDER BY total_sessions DESC;