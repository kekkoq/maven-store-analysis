-- to calculate the click-through rate (CTR) from the /products page to the /the-original-mr-fuzzy page or /the-forever-love-bear), segmented by two time periods

WITH product_funnel AS (
    SELECT
        wp.website_session_id,
        wp.website_pageview_id,
        wp.pageview_url,
        CASE 
            WHEN wp.created_at < '2013-01-06' THEN 'pre_product'
            ELSE 'post_product' 
        END AS period,
        -- to get the URL of the *next* pageview in each session 
        LEAD(wp.pageview_url, 1) OVER (
            PARTITION BY wp.website_session_id 
            ORDER BY wp.website_pageview_id
        ) AS next_page_url
        
    FROM website_pageviews wp
    WHERE wp.created_at BETWEEN '2012-10-06' AND '2013-04-06'
)
SELECT 
    pf.period,
    
    -- 1. Total sessions that viewed the /products page
    COUNT(pf.website_session_id) AS total_sessions_on_products,
    
    -- 2. Sessions that clicked from /products to either product page
    COUNT(CASE 
        WHEN pf.next_page_url IN ('/the-original-mr-fuzzy', '/the-forever-love-bear') 
        THEN 1 ELSE NULL 
    END) AS next_page_clicks,
    
    -- 3. Percentage: Clicks to Next Page / Total Sessions
    CAST(COUNT(CASE 
        WHEN pf.next_page_url IN ('/the-original-mr-fuzzy', '/the-forever-love-bear') 
        THEN 1 END) AS REAL) * 100.0 / COUNT(pf.website_session_id) AS percent_next_page,
        
    -- 4. Sessions that clicked to the 'original' product page
    COUNT(CASE 
        WHEN pf.next_page_url = '/the-original-mr-fuzzy' 
        THEN 1 ELSE NULL 
    END) AS original_product_clicks,
    
    -- 5. Sessions that clicked to the 'forever' product page
    COUNT(CASE 
        WHEN pf.next_page_url = '/the-forever-love-bear' 
        THEN 1 ELSE NULL 
    END) AS forever_product_clicks
    
FROM product_funnel AS pf
WHERE pf.pageview_url = '/products' -- Filter for the start of the funnel here
GROUP BY pf.period
ORDER BY pf.period DESC;