SELECT
  DISTINCT DATE(_partitiontime)
FROM
  `g-project.integration.fd_booking_all` a
WHERE
_partitiontime >=TIMESTAMP(DATE_SUB(current_date,INTERVAL 30 day))
AND _partitiontime < TIMESTAMP(current_date('Asia/Jakarta'))
AND DATE(load_time,'Asia/Jakarta') = 'dstart'
order by 1