select count(1) as count, date(booking_creation_time) as date
from `g-project.playground.booking_log`
where date(booking_creation_time) >= 'dstart' and date(booking_creation_time) < 'dend'
group by date