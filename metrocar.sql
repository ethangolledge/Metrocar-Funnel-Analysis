[1]
-- Examining the funnel at a user level granularity

WITH
 user_funnel AS (
 SELECT
 1 AS step,
 'app_downloads' AS funnel_name,
 COUNT(DISTINCT app_download_key)
 FROM
 app_downloads
 UNION
 SELECT
 2 AS step,
 'signups' AS funnel_name,
 COUNT(DISTINCT user_id)
 FROM
 signups
 UNION
 SELECT
 3 AS step,
 'ride_requested' AS funnel_name,
 COUNT(DISTINCT user_id)
 FROM
 ride_requests
 UNION
 SELECT
 4 AS step,
 'ride_accepted' AS funnel_name,
 COUNT(DISTINCT user_id)
 FROM
 ride_requests
 WHERE
 accept_ts IS NOT NULL
 UNION
 SELECT
 5 AS step,
 'ride_completed',
 COUNT(DISTINCT user_id)
 FROM ride_requests
 WHERE dropoff_ts IS NOT NULL
 UNION
 SELECT
 6 AS step,
 'approved_transaction' AS funnel_name,
 COUNT(DISTINCT r.user_id)
 FROM
 transactions t
 LEFT JOIN ride_requests r
 ON r.ride_id = t.ride_id
 WHERE
 charge_status = 'Approved'
 UNION
 SELECT
 7 AS step,
 'reviewed' AS funnel_name,
 COUNT(DISTINCT re.user_id)
 FROM reviews re

 )
SELECT
 funnel_name,
 COUNT AS user_count,
 COUNT::FLOAT / LAG(COUNT) OVER (
 ORDER BY step
 ) AS perc_previous,
 COUNT::FLOAT / LAG(COUNT, 6) OVER (
 ORDER BY step
 ) AS perc_top
FROM
 user_funnel
ORDER BY
 step;



[2]
-- Examining the funnel at a ride level granularity
WITH
 ride_funnel AS (
 SELECT
 1 AS step,
 'app_downloads' AS funnel_name,
 CAST(NULL AS BIGINT) AS count
 FROM
 app_downloads
 UNION
 SELECT
 2 AS step,
 'signups' AS funnel_name,
 CAST(NULL AS BIGINT) AS count
 FROM
 signups
 UNION
 SELECT
 3 AS step,
 'ride_requested' AS funnel_name,
 COUNT(DISTINCT ride_id) AS count
 FROM
 ride_requests
 UNION
 SELECT
 4 AS step,
 'ride_accepted' AS funnel_name,
 COUNT(DISTINCT ride_id) AS count
 FROM
 ride_requests
 WHERE
 accept_ts IS NOT NULL
 UNION
 SELECT
 5 AS step,
 'ride_completed' AS funnel_name,
 COUNT(DISTINCT ride_id) AS count
 FROM ride_requests
 WHERE
 dropoff_ts IS NOT NULL
 UNION
 SELECT
 6 AS step,
 'approved_transaction' AS funnel_name,
 COUNT(DISTINCT ride_id) AS count
 FROM
 transactions
 WHERE
 charge_status = 'Approved'
 UNION
 SELECT
 7 AS step,
 'reviewed' AS funnel_name,
 COUNT(DISTINCT ride_id) AS count
 FROM reviews

 )
SELECT
 funnel_name,
 COUNT AS ride_count,
 COUNT::FLOAT / LAG(COUNT) OVER (
 ORDER BY step
 ) AS perc_previous,
 COUNT::FLOAT / LAG(COUNT, 4) OVER (
 ORDER BY step
 ) AS perc_top
FROM
 ride_funnel
ORDER BY
 step;



[3]
--User level granularity query data for Tableau
WITH
 user_downloads AS (
 SELECT
 1 AS funnel_step,
 'downloads' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT app_download_key) AS user_count,
 CAST(NULL AS numeric) AS ride_count
 FROM
 app_downloads a
 LEFT JOIN signups s ON a.app_download_key = s.session_id
 GROUP BY
 platform,
 age_range,
 year,
 month,
 hour
 ),
 user_signups AS (
 SELECT
 2 AS funnel_step,
 'signups' AS funnel_name,
 platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT user_id) AS user_count,
 CAST(NULL AS numeric) AS ride_count
 FROM
 signups s
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 user_ride_requests AS (
 SELECT
 3 AS funnel_step,
 'ride_requested' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT r.user_id) AS user_count,
 COUNT(r.user_id) AS ride_count
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 user_ride_accepted AS (
 SELECT
 4 AS funnel_step,
 'ride_accepted' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT r.user_id) AS user_count,
 COUNT(r.user_id) AS ride_count
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 WHERE
 accept_ts IS NOT NULL
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 user_ride_completed AS (
 SELECT
 5 AS funnel_step,
 'ride_completed' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT r.user_id) AS user_count,
 COUNT(r.user_id) AS ride_count
 FROM
 ride_requests r
 LEFT JOIN transactions t ON r.ride_id = t.ride_id
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 WHERE
 cancel_ts IS NULL
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 user_payment AS (
 SELECT
 6 AS funnel_step,
 'payment' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT r.user_id) AS user_count,
 COUNT(r.user_id) AS ride_count
 FROM
 transactions t
 LEFT JOIN ride_requests r ON t.ride_id = r.ride_id
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 WHERE
 t.charge_status = 'Approved'
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 user_review AS (
 SELECT
 7 AS funnel_step,
 'review' AS funnel_name,
 a.platform,
 s.age_range,
 EXTRACT(
 YEAR
 FROM
 download_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 download_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 download_ts
 ) AS hour,
 COUNT(DISTINCT re.user_id) AS user_count,
 COUNT(re.user_id) AS ride_count
 FROM
 reviews re
 LEFT JOIN ride_requests r ON re.ride_id = r.ride_id
 LEFT JOIN signups s ON re.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 )
SELECT
 *
FROM
 user_downloads
UNION
SELECT
 *
FROM
 user_signups
UNION
SELECT

*
FROM
 user_ride_requests
UNION
SELECT

*
FROM
 user_ride_accepted
UNION
SELECT

*
FROM
 user_ride_completed
UNION
SELECT

*
FROM
 user_payment
UNION
SELECT

*
FROM
 user_review
ORDER BY
 funnel_step;




-- Ride level granularity querying data for Tableau
[4]

WITH
 rides_requested AS (
 SELECT
 3 AS funnel_step,
 'ride_requested' AS funnel_name,
 COUNT(DISTINCT r.ride_id) AS ride_count,
 a.platform,
 COALESCE(s.age_range, 'Not specified') AS age_range,
 EXTRACT(
 YEAR
 FROM
 r.request_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 r.request_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 r.request_ts
 ) AS hour,
 CAST(NULL AS numeric) AS avg_time_requested_pickup,
 CAST(NULL AS numeric) AS avg_time_requested_accepted,
 CAST(NULL AS numeric) AS avg_time_accepted_pickup,
 ROUND(
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.cancel_ts - r.request_ts)
 )
 ),
 2
 ) AS avg_time_ride_requested_cancelled,
 CAST(NULL AS numeric) AS avg_time_accepted_cancelled,
 (SUM(
 CASE
 WHEN r.cancel_ts IS NOT NULL THEN 1
 ELSE 0
 END
 ))::float / NULLIF(COUNT(r.request_ts), 0) AS proportion_of_cancellations,
 CAST(NULL AS numeric) AS avg_ride_duration,
 CAST(NULL AS numeric) AS sum_purchase_amount,
 CAST(NULL AS numeric) AS avg_rating
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 rides_accepted AS (
 SELECT
 4 AS funnel_step,
 'ride_accepted' AS funnel_name,
 COUNT(DISTINCT r.ride_id) AS ride_count,
 a.platform,
 COALESCE(s.age_range, 'Not specified') AS age_range,
 EXTRACT(
 YEAR
 FROM
 r.request_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 r.request_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 r.request_ts
 ) AS hour,
 CAST(NULL AS numeric) AS avg_time_requested_pickup,
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.accept_ts - r.request_ts)
 )
 ) AS avg_time_requested_accepted,
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.pickup_ts - r.accept_ts)
 )
 ) AS avg_time_accepted_pickup,
 CAST(NULL AS numeric) AS avg_time_ride_requested_cancelled,
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.cancel_ts - r.accept_ts)
 )
 ) AS avg_time_accepted_cancelled,
 CAST(NULL AS float) AS proportion_of_cancellations,
 CAST(NULL AS numeric) AS avg_ride_duration,
 CAST(NULL AS numeric) AS sum_purchase_amount,
 CAST(NULL AS numeric) AS avg_rating
51
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 WHERE
 r.accept_ts IS NOT NULL
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 rides_completed AS (
 SELECT
 5 AS funnel_step,
 'ride_completed' AS funnel_name,
 COUNT(DISTINCT r.ride_id) AS ride_count,
 a.platform,
 COALESCE(s.age_range, 'Not specified') AS age_range,
 EXTRACT(
 YEAR
 FROM
 r.request_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 r.request_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 r.request_ts
 ) AS hour,
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.pickup_ts - r.request_ts)
 )
 ) AS avg_time_requested_pickup,
 CAST(NULL AS numeric) AS avg_time_requested_accepted,
 CAST(NULL AS numeric) AS avg_time_accepted_pickup,
 CAST(NULL AS numeric) AS avg_time_ride_requested_cancelled,
 CAST(NULL AS numeric) AS avg_time_accepted_cancelled,
 CAST(NULL AS float) AS proportion_of_cancellations,
 AVG(
 EXTRACT(
 MINUTE
 FROM
 (r.dropoff_ts - r.pickup_ts)
 )
 ) AS avg_ride_duration,
 CAST(NULL AS numeric) AS sum_purchase_amount,
 CAST(NULL AS numeric) AS avg_rating
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 WHERE
 r.accept_ts IS NOT NULL
 AND r.dropoff_ts IS NOT NULL
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 rides_paid AS (
 SELECT
 6 AS funnel_step,
 'payment' AS funnel_name,
 COUNT(DISTINCT r.ride_id) AS ride_count,
 a.platform,
 COALESCE(s.age_range, 'Not specified') AS age_range,
 EXTRACT(
 YEAR
 FROM
 r.request_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 r.request_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 r.request_ts
 ) AS hour,
 CAST(NULL AS numeric) AS avg_time_requested_pickup,
 CAST(NULL AS numeric) AS avg_time_requested_accepted,
 CAST(NULL AS numeric) AS avg_time_requested_accepted_pickup,
 CAST(NULL AS numeric) AS avg_time_ride_requested_cancelled,
 CAST(NULL AS numeric) AS avg_time_accepted_cancelled,
 CAST(NULL AS float) AS proportion_of_cancellations,
 CAST(NULL AS numeric) AS avg_ride_duration,
 SUM(t.purchase_amount_usd) AS sum_purchase_amount,
 CAST(NULL AS numeric) AS avg_rating
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 LEFT JOIN transactions t ON r.ride_id = t.ride_id
 WHERE
 r.accept_ts IS NOT NULL
 AND r.dropoff_ts IS NOT NULL
 AND t.charge_status = 'Approved'
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour
 ),
 rides_reviewed AS (
 SELECT
 7 AS funnel_step,
 'ride reviewed' AS funnel_name,
 COUNT(DISTINCT r.ride_id) AS ride_count,
 a.platform,
 COALESCE(s.age_range, 'Not specified') AS age_range,
 EXTRACT(
 YEAR
 FROM
 r.request_ts
 ) AS year,
 EXTRACT(
 MONTH
 FROM
 r.request_ts
 ) AS month,
 EXTRACT(
 HOUR
 FROM
 r.request_ts
 ) AS hour,
 CAST(NULL AS numeric) AS avg_time_requested_pickup,
 CAST(NULL AS numeric) AS avg_time_requested_accepted,
 CAST(NULL AS numeric) AS avg_time_accepted_pickup,
 CAST(NULL AS numeric) AS avg_time_ride_requested_cancelled,
 CAST(NULL AS numeric) AS avg_time_accepted_cancelled,
 CAST(NULL AS float) AS proportion_of_cancellations,
 CAST(NULL AS numeric) AS avg_ride_duration,
 CAST(NULL AS numeric) AS sum_purchase_amount,
 AVG(re.rating) AS avg_rating
 FROM
 ride_requests r
 LEFT JOIN signups s ON r.user_id = s.user_id
 LEFT JOIN app_downloads a ON s.session_id = a.app_download_key
 LEFT JOIN transactions t ON r.ride_id = t.ride_id
 LEFT JOIN reviews re ON t.ride_id = re.ride_id
 WHERE
 r.accept_ts IS NOT NULL
 AND r.dropoff_ts IS NOT NULL
 AND t.charge_status = 'Approved'
 GROUP BY
 a.platform,
 s.age_range,
 year,
 month,
 hour

)
SELECT

*
FROM
 rides_requested
UNION
SELECT

*
FROM
 rides_accepted
UNION
SELECT

*
FROM
 rides_completed
UNION
SELECT

*
FROM
 rides_paid
UNION
SELECT

*
FROM
 rides_reviewed



-- Reviews querying data for manipulation in python
WITH user_reviews AS (
 SELECT
 re.user_id,
 DATE_PART('year', request_ts) AS year,
 DATE_PART('month', request_ts) AS month,
 DATE_PART('hour', request_ts) AS hour,
 re.rating,
 re.review
 FROM
 reviews re
 LEFT JOIN ride_requests r ON re.ride_id = r.ride_id
)
SELECT
 user_id,
year,
 month,
 hour,
 rating,
 review
FROM user_reviews
ORDER BY
 random()
LIMIT 49999;