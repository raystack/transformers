DECLARE power INT64;
SET power = 9001;

WITH simple_sel as (
    SELECT * from `g-project.playground.sample_select`
    WHERE `over` = 9001
)
select * from simple_sel;