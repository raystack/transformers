MERGE `destination_table` S
using
(
select count(1) as count, date(booking_creation_time) as date
from `g-project.playground.booking_log`
where date(booking_creation_time) >= 'dstart' and date(booking_creation_time) < 'dend'
group by date
) N
on S.date = N.date
WHEN MATCHED then
UPDATE SET `count` = N.count
when not matched then
INSERT (`date`, `count`) VALUES(N.date, N.count)
