select 
"beerus" as hakai, 
"naruto" as rasengan, 
EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `over`, 
CAST("__execution_time__" AS TIMESTAMP) as `load_timestamp`;