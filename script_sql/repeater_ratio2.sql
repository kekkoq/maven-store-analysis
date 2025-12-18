WITH user_session_data AS (
    SELECT
        user_id,
        created_at,
        is_repeat_session,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at ASC) AS session_num
    FROM website_sessions
    WHERE created_at BETWEEN '2014-01-01' AND '2014-11-04'
),
time_to_return AS (
    SELECT
        usd.user_id,
        usd.created_at AS first_session_date,
        MIN(next_session.created_at) AS second_session_date
    FROM user_session_data AS usd
    LEFT JOIN user_session_data AS next_session
        ON usd.user_id = next_session.user_id
        AND usd.session_num = 1
        AND next_session.session_num = 2 
    WHERE usd.session_num = 1 
      AND usd.is_repeat_session = 0 -- Ensures the first session was an acquisition (our cohort)
    GROUP BY 
        usd.user_id, 
        usd.created_at -- Group by user to ensure the outer select works
)
SELECT
    AVG(JULIANDAY(second_session_date) - JULIANDAY(first_session_date)) AS avg_days_to_return,
    MIN(JULIANDAY(second_session_date) - JULIANDAY(first_session_date)) AS min_days_to_return,
    MAX(JULIANDAY(second_session_date) - JULIANDAY(first_session_date)) AS max_days_to_return,
    -- Total count of users in the cohort who *had* a second session
    COUNT(CASE WHEN second_session_date IS NOT NULL THEN user_id END) AS users_who_returned 
FROM time_to_return;