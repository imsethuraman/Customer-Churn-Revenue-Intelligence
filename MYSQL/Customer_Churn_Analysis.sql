/* =========================================================
   PROJECT: RavenStack SaaS - Customer Churn Analysis
   TOOL: MySQL
   DESCRIPTION:
   End-to-end EDA to analyze churn, revenue, engagement,
   and support behavior using multiple tables.
========================================================= */

-- =========================================================
-- 1. USE DATABASE
-- =========================================================
USE ravenstack;

-- =========================================================
-- 2. DATA PREVIEW (UNDERSTAND STRUCTURE)
-- =========================================================
SELECT * FROM accounts LIMIT 5;
SELECT * FROM subscriptions LIMIT 5;
SELECT * FROM churn_events LIMIT 5;
SELECT * FROM feature_usage LIMIT 5;
SELECT * FROM support_tickets LIMIT 5;

-- =========================================================
-- 3. OVERALL CHURN RATE
-- =========================================================
/*
Purpose:
Measure business health by calculating churn %

Insight:
Higher churn = retention issue
*/
SELECT 
    COUNT(DISTINCT a.account_id) AS total_customers,
    COUNT(DISTINCT c.account_id) AS churned_customers,
    ROUND(
        COUNT(DISTINCT c.account_id) / COUNT(DISTINCT a.account_id), 2
    ) AS churn_rate
FROM accounts a
LEFT JOIN churn_events c 
    ON a.account_id = c.account_id;

-- =========================================================
-- 4. CHURN BY PLAN
-- =========================================================
/*
Purpose:
Identify which pricing plans have highest churn
*/
SELECT 
    s.plan_tier,
    COUNT(DISTINCT s.account_id) AS total_users,
    COUNT(DISTINCT c.account_id) AS churned_users,
    ROUND(
        COUNT(DISTINCT c.account_id) / COUNT(DISTINCT s.account_id), 2
    ) AS churn_rate
FROM subscriptions s
LEFT JOIN churn_events c 
    ON s.account_id = c.account_id
GROUP BY s.plan_tier
ORDER BY churn_rate DESC;

-- =========================================================
-- 5. MONTHLY REVENUE TREND
-- =========================================================
/*
Purpose:
Track revenue growth over time
*/
SELECT 
    DATE_FORMAT(start_date, '%Y-%m') AS month,
    SUM(mrr_amount) AS revenue
FROM subscriptions
GROUP BY month
ORDER BY month;

-- =========================================================
-- 6. FEATURE USAGE VS CHURN
-- =========================================================
/*
Purpose:
Understand which features reduce churn
*/
SELECT 
    f.feature_name,
    COUNT(DISTINCT s.account_id) AS users,
    COUNT(DISTINCT c.account_id) AS churned_users,
    ROUND(
        COUNT(DISTINCT c.account_id) / COUNT(DISTINCT s.account_id), 2
    ) AS churn_rate
FROM feature_usage f
LEFT JOIN subscriptions s 
    ON f.subscription_id = s.subscription_id
LEFT JOIN churn_events c 
    ON s.account_id = c.account_id
GROUP BY f.feature_name
ORDER BY churn_rate DESC;

-- =========================================================
-- 7. FEATURE USAGE SEGMENTATION
-- =========================================================
/*
Purpose:
Segment users based on feature usage intensity
*/
SELECT 
    f.feature_name,
    CASE 
        WHEN f.usage_count = 0 THEN 'No Usage'
        WHEN f.usage_count BETWEEN 1 AND 10 THEN 'Low Usage'
        ELSE 'High Usage'
    END AS usage_segment,
    COUNT(DISTINCT s.account_id) AS users,
    ROUND(
        COUNT(DISTINCT c.account_id) / COUNT(DISTINCT s.account_id), 2
    ) AS churn_rate
FROM feature_usage f
LEFT JOIN subscriptions s 
    ON f.subscription_id = s.subscription_id
LEFT JOIN churn_events c 
    ON s.account_id = c.account_id
GROUP BY f.feature_name, usage_segment
ORDER BY churn_rate DESC;

-- =========================================================
-- 8. SUPPORT IMPACT ON CHURN
-- =========================================================
/*
Purpose:
Analyze how support tickets influence churn
*/
SELECT 
    CASE 
        WHEN ticket_count = 0 THEN 'No Tickets'
        WHEN ticket_count BETWEEN 1 AND 3 THEN 'Low Support'
        ELSE 'High Support'
    END AS support_category,
    COUNT(account_id) AS users,
    SUM(churn_flag) AS churned,
    ROUND(SUM(churn_flag)/COUNT(account_id), 2) AS churn_rate
FROM (
    SELECT 
        a.account_id,
        COUNT(t.ticket_id) AS ticket_count,
        CASE WHEN c.account_id IS NOT NULL THEN 1 ELSE 0 END AS churn_flag
    FROM accounts a
    LEFT JOIN support_tickets t 
        ON a.account_id = t.account_id
    LEFT JOIN churn_events c 
        ON a.account_id = c.account_id
    GROUP BY a.account_id
) sub
GROUP BY support_category;

-- =========================================================
-- 9. CUSTOMER LIFETIME ANALYSIS
-- =========================================================
/*
Purpose:
Calculate how long customers stay
*/
SELECT 
    s.account_id,
    MIN(s.start_date) AS first_subscription_date,
    MAX(c.churn_date) AS churn_date,
    DATEDIFF(
        IFNULL(MAX(c.churn_date), CURDATE()),
        MIN(s.start_date)
    ) AS lifetime_days
FROM subscriptions s
LEFT JOIN churn_events c 
    ON s.account_id = c.account_id
GROUP BY s.account_id;

-- =========================================================
-- 10. USER ACTIVITY ANALYSIS
-- =========================================================
/*
Purpose:
Measure engagement level per user
*/
SELECT 
    s.account_id,
    COUNT(*) AS activity_count
FROM feature_usage f
LEFT JOIN subscriptions s 
    ON f.subscription_id = s.subscription_id
GROUP BY s.account_id
ORDER BY activity_count DESC;

-- =========================================================
-- 11. FINAL DATASET
-- =========================================================
/*
Purpose:
Create a single dataset for dashboarding
*/
WITH subscription_cte AS (
    SELECT 
        account_id,
        MIN(start_date) AS first_subscription_date,
        SUM(mrr_amount) AS total_mrr
    FROM subscriptions
    GROUP BY account_id
),

activity_cte AS (
    SELECT 
        s.account_id,
        COUNT(*) AS activity_count
    FROM feature_usage f
    LEFT JOIN subscriptions s 
        ON f.subscription_id = s.subscription_id
    GROUP BY s.account_id
),

support_cte AS (
    SELECT 
        account_id,
        COUNT(ticket_id) AS total_tickets
    FROM support_tickets
    GROUP BY account_id
),

churn_cte AS (
    SELECT DISTINCT 
        account_id,
        1 AS churn_flag
    FROM churn_events
)

SELECT 
    a.account_id,
    a.country,
    sub.total_mrr,
    act.activity_count,
    sup.total_tickets,

    CASE 
        WHEN act.activity_count < 20 THEN 'Low'
        ELSE 'High'
    END AS activity_segment,

    CASE 
        WHEN sup.total_tickets > 3 THEN 'High Support'
        ELSE 'Low Support'
    END AS support_segment,

    CASE 
        WHEN c.churn_flag = 1 THEN 'Churned'
        ELSE 'Active'
    END AS churn_status

FROM accounts a
LEFT JOIN subscription_cte sub 
    ON a.account_id = sub.account_id
LEFT JOIN activity_cte act 
    ON a.account_id = act.account_id
LEFT JOIN support_cte sup 
    ON a.account_id = sup.account_id
LEFT JOIN churn_cte c 
    ON a.account_id = c.account_id;

-- =========================================================
-- 12. FINAL DATASET
-- =========================================================
/*
Purpose:
Final dataset combining all metrics for dashboarding
*/

WITH subscription_cte AS (
    SELECT 
        account_id,
        MIN(start_date) AS first_subscription_date,
        MAX(end_date) AS last_subscription_date,
        MAX(plan_tier) AS plan_tier,
        SUM(mrr_amount) AS total_mrr,
        SUM(arr_amount) AS total_arr
    FROM subscriptions
    GROUP BY account_id
),

activity_cte AS (
    SELECT 
        s.account_id,
        COUNT(f.usage_id) AS total_usage,
        SUM(f.usage_count) AS total_feature_usage,
        AVG(f.usage_duration_secs) AS avg_usage_duration,
        SUM(f.error_count) AS total_errors
    FROM feature_usage f
    LEFT JOIN subscriptions s 
        ON f.subscription_id = s.subscription_id
    GROUP BY s.account_id
),

support_cte AS (
    SELECT 
        account_id,
        COUNT(ticket_id) AS total_tickets,
        AVG(resolution_time_hours) AS avg_resolution_time,
        AVG(first_response_time_minutes) AS avg_first_response,
        AVG(satisfaction_score) AS avg_satisfaction
    FROM support_tickets
    GROUP BY account_id
),

churn_cte AS (
    SELECT 
        account_id,
        MAX(churn_date) AS churn_date,
        COUNT(*) AS churn_events_count
    FROM churn_events
    GROUP BY account_id
),

final_eda AS (
    SELECT 
        a.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.signup_date,
        a.plan_tier AS initial_plan,
        a.is_trial,

        sub.plan_tier AS current_plan,
        sub.total_mrr,
        sub.total_arr,

        act.total_usage,
        act.total_feature_usage,
        act.avg_usage_duration,
        act.total_errors,

        sup.total_tickets,
        sup.avg_resolution_time,
        sup.avg_first_response,
        sup.avg_satisfaction,

        ch.churn_date,
        ch.churn_events_count,

        -- Lifetime
        DATEDIFF(
            IFNULL(ch.churn_date, CURDATE()),
            sub.first_subscription_date
        ) AS lifetime_days,

        -- Churn Flag
        CASE 
            WHEN ch.account_id IS NOT NULL THEN 1 
            ELSE 0 
        END AS churn_flag

    FROM accounts a
    LEFT JOIN subscription_cte sub ON a.account_id = sub.account_id
    LEFT JOIN activity_cte act ON a.account_id = act.account_id
    LEFT JOIN support_cte sup ON a.account_id = sup.account_id
    LEFT JOIN churn_cte ch ON a.account_id = ch.account_id
)

SELECT * FROM final_eda;

-- =========================================================
-- END OF PROJECT
-- =========================================================