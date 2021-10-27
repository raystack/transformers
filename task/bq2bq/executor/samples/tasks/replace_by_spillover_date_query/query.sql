select
    TIMESTAMP('__dstart__') as dstart,
    TIMESTAMP('__dend__') as dend,
    "beerus" as hakai,
    "naruto" as rasengan,
    CAST("__execution_time__" AS TIMESTAMP) as `load_timestamp`
