SELECT
session_hour,
ROUND(AVG(CASE WHEN session_week = '0' THEN website_session_id ELSE NULL END), 0) AS SUNDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '1' THEN website_session_id ELSE NULL END), 0) AS MONDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '2' THEN website_session_id ELSE NULL END), 0) AS TUESDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '3' THEN website_session_id ELSE NULL END), 0) AS WEDNESDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '4' THEN website_session_id ELSE NULL END), 0) AS THURSDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '5' THEN website_session_id ELSE NULL END), 0) AS FRIDAY_SESSIONS,
ROUND(AVG(CASE WHEN session_week = '6' THEN website_session_id ELSE NULL END), 0) AS SATURDAY_SESSIONS
FROM 
    (SELECT 
        DATE(created_at) AS session_date,
        STRFTIME('%w', created_at) AS session_week,
        STRFTIME('%H', created_at) AS session_hour,
        COUNT(website_session_id) AS website_session_id
        FROM website_sessions
        WHERE created_at BETWEEN '2012-09-15' AND '2012-11-15' 
        GROUP BY 1,2,3
    ) AS weekly_sessions
GROUP BY session_hour
    
