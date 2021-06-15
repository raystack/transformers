import re
from datetime import timedelta
from bumblebee.window import Window
import sqlparse

MERGE_AUTO_REPLACE_SCRIPT_QUERY_TEMPLATE = """\
-- Optimus generated
DECLARE partitions ARRAY<DATE>;

{header}

CREATE TEMP TABLE `opt__partitions` AS (
  {sql_query}
);

SET (partitions) = (
    SELECT AS STRUCT
        array_agg(DISTINCT DATE(`{partition_column_name}`))
    FROM opt__partitions
);

MERGE INTO
  `{destination_table}` AS target
USING
  (
      Select * from `opt__partitions`
  ) AS source
ON FALSE
WHEN NOT MATCHED BY SOURCE AND DATE(`{partition_column_name}`) IN UNNEST(partitions)
THEN DELETE
WHEN NOT MATCHED THEN INSERT
  (
     {destination_columns}
  )
VALUES
  (
      {source_columns}
  );
"""

MERGE_REPLACE_WITH_FILTER_QUERY_TEMPLATE = """\
-- Optimus generated
{header}

MERGE INTO
  `{destination_table}` AS target
USING
  (
      {sql_query}
  ) AS source
ON FALSE
WHEN NOT MATCHED BY SOURCE AND {filter_expression}
THEN DELETE
WHEN NOT MATCHED THEN INSERT
  (
     {destination_columns}
  )
VALUES
  (
      {source_columns}
  );
"""


class QueryParameter(dict):
    def __init__(self):
        super().__init__()


class WindowParameter(QueryParameter):
    def __init__(self, window: Window):
        super().__init__()
        dstart_regex = r"__dstart__"
        dend_regex = r"__dend__"

        # convert time to date if hour is no required
        params = {
            dstart_regex: window.start.strftime("%Y-%m-%d"),
            dend_regex: window.end.strftime("%Y-%m-%d"),
        }
        if window.size < timedelta(seconds=60 * 60 * 24) or window.truncate_upto == "h":
            params = {
                dstart_regex: window.start.strftime("%Y-%m-%d %H:%M:%S"),
                dend_regex: window.end.strftime("%Y-%m-%d %H:%M:%S"),
            }

        self.update(params)


class DestinationParameter(QueryParameter):
    def __init__(self, full_table_name):
        super().__init__()
        self.update({r"(__destination_table__)": full_table_name})


class ExecutionParameter(QueryParameter):
    def __init__(self, exec_time):
        super().__init__()
        self.update({r"(__execution_time__)": exec_time.strftime('%Y-%m-%dT%H:%M:%S.%f')})


# this should be deprecated as optimus does the macro conversion now
class Query(str):

    def replace_param(self, param_kv):
        temp = self
        for key, value in param_kv.items():
            temp = re.sub(key, value, temp, 0, re.MULTILINE)
        return Query(temp)

    def apply_parameter(self, query_parameter: QueryParameter):
        return self.replace_param(query_parameter)

    def print(self):
        print(self)

    def print_with_logger(self, log):
        log.info("sql transformation query:\n{}".format(self))


class MergeReplaceQuery(str):

    def from_filter(self, destination_table: str, destination_columns: list, source_columns: list, filter: str):
        prepared_destination_columns = self.prepare_column_names(destination_columns)
        prepared_source_columns = self.prepare_column_names(source_columns)
        header, body = self.parsed_sql()

        q = MERGE_REPLACE_WITH_FILTER_QUERY_TEMPLATE.format(header="\n".join(header), sql_query=body,
                                                            destination_table=destination_table,
                                                            destination_columns=",".join(prepared_destination_columns),
                                                            source_columns=",".join(prepared_source_columns),
                                                            filter_expression=filter)
        return MergeReplaceQuery(q)

    def auto(self, destination_table: str, destination_columns: list, source_columns: list,
             partition_column_name: str, partition_column_type: str):
        prepared_destination_columns = self.prepare_column_names(destination_columns)
        prepared_source_columns = self.prepare_column_names(source_columns)
        header, body = self.parsed_sql()

        q = MERGE_AUTO_REPLACE_SCRIPT_QUERY_TEMPLATE.format(header="\n".join(header), sql_query=body,
                                                            destination_table=destination_table,
                                                            destination_columns=",".join(prepared_destination_columns),
                                                            source_columns=",".join(prepared_source_columns),
                                                            partition_column_name=partition_column_name,
                                                            partition_column_type=partition_column_type)
        return MergeReplaceQuery(q)

    def prepare_column_names(self, colums):
        prepared_columns = []
        for col in colums:
            prepared_columns.append("`{}`".format(col))
        return prepared_columns

    def parsed_sql(self):
        headers = []  # create function queries
        body = []  # with/statement queries

        # split into multiple queries seperated my semicolons
        queries = sqlparse.split(self)
        if len(queries) == 1:
            return headers, queries[0]

        for query in queries:
            # parse will provide a AST
            # read through all tokens if any of them is a DDL stmt
            tokens = sqlparse.parse(query)[0].tokens
            is_ddl = False
            for token in tokens:
                if token.ttype == sqlparse.tokens.Keyword.DDL:
                    is_ddl = True
                    break
            if is_ddl:
                headers.append(query)
            else:
                body.append(query)
        if len(body) != 1:
            raise Exception("invalid replace query, should have exactly one DML/CTE statements")
        return headers, body[0]

    def print(self):
        print(self)

    def print_with_logger(self, log):
        log.info("sql transformation query:\n{}".format(self))
