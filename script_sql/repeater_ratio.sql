
WITH new_sessions AS (
    -- 1. Identify the *first* (new) session for all users in the cohort window
    SELECT
        user_id,
        website_session_id AS first_session_id,
        created_at AS first_session_date
    FROM website_sessions
    WHERE created_at BETWEEN '2014-01-01' AND '2014-11-04'
    AND is_repeat_session = 0
),
first_second_session_diff AS (
    SELECT
        ns.user_id,
        JULIANDAY(MIN(ws.created_at)) - JULIANDAY(ns.first_session_date) AS days_to_first_repeat
    FROM new_sessions AS ns
    LEFT JOIN website_sessions AS ws
        ON ns.user_id = ws.user_id
       AND ws.website_session_id > ns.first_session_id 
        AND ws.created_at BETWEEN '2014-01-01' AND '2014-11-04'
    GROUP BY 
        ns.user_id,
        ns.first_session_date
    HAVING days_to_first_repeat IS NOT NULL  -- Only consider users who had a repeat session. Since MIN is used in the calculation, it needs to be filtered here. 
)
SELECT
    -- Use COALESCE/NULLIF to prevent division by zero, although AVG handles NULLs fine.
    AVG(days_to_first_repeat) AS avg_days_to_return,
    MIN(days_to_first_repeat) AS min_days_to_return,
    MAX(days_to_first_repeat) AS max_days_to_return,
    COUNT(user_id) AS total_users_who_returned
FROM first_second_session_diff;

