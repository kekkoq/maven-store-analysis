 WITH session_first_page AS (
    SELECT
        website_session_id,
        pageview_url,
        -- Use ROW_NUMBER() to identify the first pageview for each session
        ROW_NUMBER() OVER (
            PARTITION BY website_session_id 
            ORDER BY website_pageview_id
        ) AS rn
    FROM website_pageviews
    WHERE created_at < '2012-06-14'
),
-- Count the number of pageviews per session
session_page_counts AS (
    SELECT
        website_session_id,
        COUNT(website_pageview_id) AS pageview_count
    FROM website_pageviews
    WHERE created_at < '2012-06-14'
    GROUP BY website_session_id
)
SELECT
    COUNT(t1.website_session_id) AS total_sessions,
    SUM(CASE WHEN t2.pageview_count = 1 THEN 1 ELSE 0 END) AS bounced_sessions,
    ROUND(
        (SUM(CASE WHEN t2.pageview_count = 1 THEN 1.0 ELSE 0 END) / COUNT(t1.website_session_id)), 
        4
    ) AS bounce_rate
FROM session_first_page AS t1
INNER JOIN session_page_counts AS t2
    ON t1.website_session_id = t2.website_session_id
WHERE t1.rn = 1;