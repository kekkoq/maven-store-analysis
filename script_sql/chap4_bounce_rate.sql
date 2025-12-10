With first_page AS (
    SELECT
    website_session_id AS session_id,
    MIN(website_pageview_id) AS first_pageview_id
    FROM website_pageviews
    WHERE created_at < '2012-06-14'
    GROUP BY website_session_id),
landing_pages AS (
    SELECT
    f.session_id,
    wp.pageview_url AS landing_page
    FROM first_page AS f
    INNER JOIN website_pageviews AS wp
    ON f.first_pageview_id = wp.website_pageview_id
),
bounced_sessions AS (
    SELECT
    lp.session_id AS bounced_session_id,
    lp.landing_page,
    COUNT(wp.website_pageview_id) AS pageview_count
    FROM landing_pages AS lp
    left JOIN website_pageviews AS wp
    ON lp.session_id = wp.website_session_id
    GROUP BY lp.session_id, lp.landing_page
    HAVING COUNT(wp.website_pageview_id) = 1
)
SELECT
    COUNT(lp.session_id) AS total_sessions,
    COUNT(bs.bounced_session_id) AS bounced_sessions,
    ROUND(COUNT(bs.bounced_session_id) * 1.0 / COUNT(lp.session_id), 4) AS bounce_rate
FROM LANDING_PAGES AS lp
LEFT JOIN bounced_sessions AS bs
ON lp.session_id = bs.bounced_session_id;