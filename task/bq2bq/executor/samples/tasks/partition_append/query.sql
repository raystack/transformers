SELECT hakai, rasengan, `over`, load_timestamp as event_timestamp
FROM `g-project.playground.sample_select`
 WHERE CAST(load_timestamp AS DATETIME) >= CAST('dstart' AS DATETIME) and CAST(load_timestamp AS DATETIME) < CAST('dend' AS DATETIME)