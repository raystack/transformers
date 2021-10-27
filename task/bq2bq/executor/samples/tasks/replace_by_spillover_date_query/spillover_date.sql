SELECT
    date
FROM
    `g-project.playground.calendar_date`
-- this query will generate 14 calendar dates which we will replace 14 partitions using the main query
WHERE date >= DATE_SUB('__dstart__', INTERVAL 14 DAY)
  AND date < '__dend__'
ORDER BY date