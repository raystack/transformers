select
    `hakai`,
    `rasengan`,
    `over`,
    TIMESTAMP ('2021-09-01T01:02:03') as `event_timestamp`
from
    `g-project.playground.sample_select`
WHERE
    DATE(`load_timestamp`) >= DATE('2021-09-01')
    AND DATE(`load_timestamp`) < DATE('2021-09-30')