
WITH product_pageviews AS (
    SELECT 
        website_session_id,
        pageview_url,
        CASE WHEN created_at < '2013-01-06' THEN 'pre_production'
            WHEN created_at >= '2013-01-06' THEN 'post_production'
            ELSE NULL END AS pageview_timeframe,
        LEAD(pageview_url, 1) OVER (PARTITION BY website_session_id ORDER BY website_pageview_id) AS next_pageview_url  
    FROM website_pageviews
    WHERE CREATED_AT <= '2013-04-06'

)
SELECT 
    pageview_timeframe AS period,
    COUNT(DISTINCT website_session_id) AS total_product_pageviews,
    COUNT(DISTINCT CASE WHEN next_pageview_url IN ('/the-original-mr-fuzzy','/the-forever-love-bear') THEN website_session_id END) AS total_next_pageviews,
    COUNT(DISTINCT CASE WHEN next_pageview_url IN ('/the-original-mr-fuzzy','/the-forever-love-bear') THEN website_session_id END)*1.0
    /COUNT(DISTINCT website_session_id) AS click_through_rate_from_products,
    COUNT(DISTINCT CASE WHEN next_pageview_url = '/the-original-mr-fuzzy' THEN website_session_id END) AS fuzzy_pageviews,
    COUNT(DISTINCT CASE WHEN next_pageview_url = '/the-original-mr-fuzzy' THEN website_session_id END)*1.0
    /COUNT(DISTINCT website_session_id) AS fuzzy_click_through_rate,
    COUNT(DISTINCT CASE WHEN next_pageview_url = '/the-forever-love-bear' THEN website_session_id END) AS forever_pageviews,
    COUNT(DISTINCT CASE WHEN next_pageview_url = '/the-forever-love-bear' THEN website_session_id END)*1.0
    /COUNT(DISTINCT website_session_id) AS forever_click_through_rate,
    FROM product_pageviews
WHERE pageview_url = '/products'
GROUP BY period
