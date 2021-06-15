Bumblebee
=========
Python library for transforming data in Bigquery

## Features

* cfg file based configuration, and bigquery Standard SQL syntax for transformation
* HOURLY, DAILY, WEEKLY, MONTHLY transformation window
* Support APPEND, REPLACE and MERGE load method
* Support transformation for partitioned tables such as partition by ingestion time (default) and partition by column
* Support datetime input in different timezone
* dry run
* Support Bigquery DML Merge statement to handle spillover case

## Setup:

### Python Environment/Virtualenv

Install virtualenv :
* `pip install virtualenv`
* `virtualenv -p python3.7 venv`

Activate virtualenv :
`source venv/bin/activate`

Exit virtualenv :
`deactivate`

Run tests:
```
python3 -m unittest tests/test_transformation.py
```

### Test using docker
Run tests in a container: (this ensures that tests run in exact same environment)
```bash
docker-compose build && docker-compose run --entrypoint="./run_tests.sh" bumblebee
```

Run locally:
```bash
docker-compose up --build
```


## How to use

### Install the library

* `git clone` this repo
* Go to repo the repo root directory
* run `pip install .` or `python setup.py install`

### Task Files

file that needed to do transformation

* properties.cfg - file that contains the transform and load configuration
* query.sql - file that contains the transformation query

#### properties.cfg

| Config Name             | Description                                                                                                     | Values                              |
| ----------------------- |-----------------------------------------------------------------------------------------------------------------| ------------------------------------|
| `PROJECT`               | google cloud platform project id of the destination bigquery table                                              | ...                                 |
| `DATASET`               | bigquery dataset name of the destination table                                                                  | ...                                 |
| `TABLE`                 | the table name of the destination table                                                                         | ...                                 |
| `TASK_WINDOW`           | window of transformation, to provide dstart and dend macros values for sql transformation                       | HOURLY, DAILY, WEEKLY, MONTHLY      |
| `TIMEZONE`              | timezone of transformation, a timezone that the input datetime will be translated to in tz database name format | UTC, Asia/Jakarta, America/New_York |
| `LOAD_METHOD`           | method to load data to the destination tables                                                                   | APPEND, REPLACE, MERGE              |


Example of `properties.cfg` config :

```ini
[DESTINATION]
PROJECT="gcp-project-id"
DATASET="dataset"
TABLE="table_name"

[TRANSFORMATION]
WINDOW_SIZE = 24h
WINDOW_OFFSET = 0
WINDOW_TRUNCATE_UPTO = d
TIMEZONE="Asia/Jakarta"

[LOAD]
LOAD_METHOD="REPLACE"
```


#### Code examples

```#!/usr/bin/env python3
import bumblebee
from datetime import datetime

def main():
    task_path = "samples/tasks/dml"
    files_folders = os.listdir(task_path)
    files = list(filter(lambda f: not os.path.isfile(f) ,files_folders))
    
    bumblebee.bq2bq_v2(files,datetime(2019, 3, 22),False)

if __name__ == "__main__":
    main()
```

#### SQL macros

* `__destination_table__` - full qualified table name used in DML statement
* `__execution_time__` - Can be replaced in place of CURRENT_TIMESTAMP() macro of BQ
* `__dstart__` - start date/datetime of the window
* `__dend__` - end date/datetime of the window

The value of `dstart` and `dend` depends on `TASK_WINDOW` config in `properties.cfg` file

| Window Name   | dstart                                                             | dend                                                                 |
| ------------- |--------------------------------------------------------------------| ---------------------------------------------------------------------|
| DAILY         | The current date taken from input, for example 2019-01-01          | The next day after dstart date 2019-01-02                            |
| WEEKLY        | Start of the week date for example : 2019-04-01                    | End date of the week , for example : 2019-04-07                      |
| MONTHLY       | Start of the month date, example : 2019-01-01                      | End date of the month, for example : 2019-01-31                      |
| HOURLY        | Datetime of the start of the hour, for example 2019-01-01 01:00:00 | Datetime the start of the next hour, for example 2019-01-01 02:00:00 |

SQL transformation query :

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '__dstart__' and date(booking_creation_time) < '__dend__'
group by dt
```

Rendered SQL for DAILY window :

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '2019-01-01' and date(booking_creation_time) < '2019-01-02'
group by dt
```

Rendered SQL for HOURLY window :
the value of `dstart` and `dend` is YYYY-mm-dd HH:MM:SS formatted datetime 

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '2019-01-01 06:00:00' and date(booking_creation_time) < '2019-01-01 07:00:00'
group by dt
```

destination_table macros :

```sql
MERGE `__destination_table__` S
using
(
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '__dstart__' and date(created_time) < '__dend__'
group by dt
) N
on S.date = N.date
WHEN MATCHED then
UPDATE SET `count` = N.count
when not matched then
INSERT (`date`, `count`) VALUES(N.date, N.count)
```

### Standard SQL

SQL select statement example :

```sql
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '__dstart__' and date(booking_creation_time) < '__dend__'
group by dt
```

DML Merge statement example :

```sql
MERGE `__destination_table__` S
using
(
select count(1) as count, date(created_time) as dt
from `project.dataset.tablename`
where date(created_time) >= '__dstart__' and date(created_time) < '__dend__'
group by dt
) N
on S.date = N.date
WHEN MATCHED then
UPDATE SET `count` = N.count
when not matched then
INSERT (`date`, `count`) VALUES(N.date, N.count)
```

#### Load Method

The way data loaded to destination table depends on the partition configuration of the destination tables

| Load Method  | No Partition                                                                                   | Partitioned Table                                                                          |
| -------------|------------------------------------------------------------------------------------------------| -------------------------------------------------------------------------------------------|
| APPEND       | Append new records to destination table                                                        | Append new records to destination table per partition based on localised start_time        |
| REPLACE      | Truncate/Clean the table before insert new records                                             | Clean records in destination partition before insert new record to new partition           |
| MERGE        | Load the data using DML Merge statement, all of the load logic lies on DML merge statement     | Load the data using DML Merge statement, all of the load logic lies on DML merge statement |