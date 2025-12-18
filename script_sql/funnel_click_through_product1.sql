WITH product_start_pageviews AS (
    -- 1. Identify the starting point for each funnel: the initial product page view.
    SELECT
        website_session_id AS session_id,
        website_pageview_id AS pageview_id,
        -- Use a cleaner name for the product in the initial CTE
        CASE
            WHEN pageview_url = '/the-original-mr-fuzzy' THEN 'Mr. Fuzzy'
            WHEN pageview_url = '/the-forever-love-bear' THEN 'Love Bear'
            ELSE 'Unknown Product'
        END AS product_seen
    FROM website_pageviews
    WHERE created_at > '2013-01-06'
      AND created_at < '2013-04-09'
      AND pageview_url IN ('/the-original-mr-fuzzy', '/the-forever-love-bear')
),
sessions_with_funnel_steps AS (
    -- 2. Join the product view with ALL subsequent pageviews in the same session,
    --    then use MAX() to aggregate all steps reached per session.
    SELECT
        pv.session_id,
        pv.product_seen,
        -- MAX() will capture '1' if the page was EVER viewed later in the session, 
        -- otherwise it will be NULL (which is ignored by MAX) or 0 (if NULLs are excluded)
        MAX(CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE 0 END) AS reached_cart,
        MAX(CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE 0 END) AS reached_shipping,
        MAX(CASE WHEN wp.pageview_url = '/billing-2' THEN 1 ELSE 0 END) AS reached_billing,
        MAX(CASE WHEN wp.pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END) AS reached_thank_you
    FROM product_start_pageviews AS pv
    LEFT JOIN website_pageviews AS wp
        ON pv.session_id = wp.website_session_id
        AND wp.website_pageview_id > pv.pageview_id -- Ensure subsequent pageviews
    GROUP BY
        pv.session_id,
        pv.product_seen
)
-- 3. Final aggregation and CORRECT counting of distinct sessions for each step.
SELECT
    product_seen,
    COUNT(session_id) AS sessions_started,
    -- CORRECT COUNTING LOGIC: Only count the session_id when the step was reached (1), 
    -- otherwise return NULL (which COUNT() ignores).
    COUNT(CASE WHEN reached_cart = 1 THEN session_id ELSE NULL END) AS add_to_cart,
    COUNT(CASE WHEN reached_shipping = 1 THEN session_id ELSE NULL END) AS proceed_to_shipping,
    COUNT(CASE WHEN reached_billing = 1 THEN session_id ELSE NULL END) AS proceed_to_billing,
    COUNT(CASE WHEN reached_thank_you = 1 THEN session_id ELSE NULL END) AS completed_purchase
FROM sessions_with_funnel_steps
GROUP BY
    product_seen;