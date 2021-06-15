SELECT hakai, rasengan, `over`, load_timestamp as event_timestamp
FROM `g-project.playground.sample_select`
 WHERE DATE(load_timestamp) >= 'dstart' and date(load_timestamp) < 'dend'