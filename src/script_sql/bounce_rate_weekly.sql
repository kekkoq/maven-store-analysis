WITH session_first_pageview AS (
    SELECT
        wp.website_session_id AS session_id,
        MIN(wp.website_pageview_id) AS first_pageview_id,
        -- Use a window function to find the total pageview count for the session.
        COUNT(wp.website_pageview_id) OVER (PARTITION BY wp.website_session_id) AS total_pageviews
    FROM website_pageviews AS wp
    INNER JOIN website_sessions AS ws
        ON wp.website_session_id = ws.website_session_id
    WHERE 
        ws.created_at BETWEEN '2012-06-01' AND '2012-08-31'
        AND ws.utm_campaign = 'nonbrand'
        AND ws.utm_source = 'gsearch'
    GROUP BY 
        wp.website_session_id, 
        wp.website_pageview_id -- Group by pageview_id to ensure the window function works correctly across all rows
),
session_details AS (
    -- Get the landing page URL and determine bounce status in one step
    SELECT
         DATE(
            wp.created_at,
            'weekday 0'
        ) AS weekly_session_start,
        sfp.session_id,
        wp.pageview_url AS landing_page,
        CASE WHEN sfp.total_pageviews = 1 THEN 1 ELSE 0 END AS is_bounced
    FROM session_first_pageview AS sfp
    INNER JOIN website_pageviews AS wp
        ON sfp.first_pageview_id = wp.website_pageview_id
    WHERE wp.pageview_url IN ('/home', '/lander-1')
)
SELECT
    sd.weekly_session_start,
    ROUND(SUM(sd.is_bounced) * 1.0 / COUNT(sd.session_id), 4) AS bounce_rate,
    COUNT(CASE WHEN sd.landing_page = '/home' THEN (sd.session_id) ELSE NULL END) AS home_sessions,
    COUNT(CASE WHEN sd.landing_page = '/lander-1' THEN (sd.session_id) ELSE NULL END) AS lander_1_sessions
FROM session_details AS sd
GROUP BY  sd.weekly_session_start
ORDER BY sd.weekly_session_start;
