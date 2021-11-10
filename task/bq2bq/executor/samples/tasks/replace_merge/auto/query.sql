select
    `hakai`,
    `rasengan`,
    `over`,
    `load_timestamp` as `event_timestamp`
from
    `g-project.playground.sample_select`
WHERE
    DATE(`load_timestamp`) >= DATE('2020-08-04')
    AND DATE(`load_timestamp`) < DATE('2020-08-08')