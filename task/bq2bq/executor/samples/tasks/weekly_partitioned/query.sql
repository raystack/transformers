SELECT DISTINCT
  DATE_TRUNC(DATE(created_timestamp,'Asia/Jakarta'), WEEK(MONDAY)) AS week_start_date,
  CAST(count(order_no) AS NUMERIC) as order_count,
  CURRENT_TIMESTAMP() AS load_timestamp,
  TIMESTAMP("__execution_time__") AS last_modified_timestamp
FROM 
  `g-project.playground.twomonths_data`
WHERE
  DATE(created_timestamp,'Asia/Jakarta') >= DATE('dstart')
  AND DATE(created_timestamp,'Asia/Jakarta') < DATE('dend')
  AND LOWER(latest_status_name) = 'completed'
GROUP BY 
  1