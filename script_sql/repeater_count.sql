-- cusstomer count by repeat sessions
SELECT
    total_sessions - 1 AS repeat_sessions, -- E.g. total session = 1 means new customer
    COUNT(user_id) AS users
FROM (
    SELECT
        user_id,
        COUNT(website_session_id) AS total_sessions
    FROM website_sessions
    WHERE created_at BETWEEN '2014-01-01' AND '2014-11-04'
    GROUP BY user_id 
    HAVING MIN(is_repeat_session) = 0 
) AS user_activity
GROUP BY 1
ORDER BY 1;