# Bigquery SQL transformation

Optimus plugin that supports variety of load methods to execute SQL transformations.
Features
- Automatic dependency resolution
- Append to a partition
- Replace table partition
- Merge statements
- BQ Scripts

#### Load Method

The way data loaded to destination table depends on the partition configuration of the destination tables

| Load Method  | No Partition                                                                                   | Partitioned Table                                                                          |
| -------------|------------------------------------------------------------------------------------------------| -------------------------------------------------------------------------------------------|
| APPEND       | Append new records to destination table                                                        | Append new records to destination table per partition based on localised start_time        |
| REPLACE      | Truncate/Clean the table before insert new records                                             | Clean records in destination partition before insert new record to new partition           |
| MERGE        | Load the data using DML Merge statement, all of the load logic lies on DML merge statement     | Load the data using DML Merge statement, all of the load logic lies on DML merge statement |
| REPLACE_MERGE| Doesn't work for non partitioned tables and partitioned tables with ingestion time             | Same as REPLACE but experimental                                                           |

Note: if `REPLACE` load method is used and window size greater than the partition delta,
it is assumed the table is partitioned with `DAY` at the moment.
This will split the query into multiple queries executing for each partition one by one.
