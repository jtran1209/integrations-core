# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from itertools import chain

import clickhouse_driver
from six import raise_from

from datadog_checks.base import AgentCheck, is_affirmative
from datadog_checks.base.utils.containers import iter_unique

from . import queries
from .exceptions import QueryExecutionError
from .utils import ErrorSanitizer


class ClickhouseCheck(AgentCheck):
    __NAMESPACE__ = 'clickhouse'
    SERVICE_CHECK_CONNECT = 'can_connect'

    def __init__(self, name, init_config, instances):
        super(ClickhouseCheck, self).__init__(name, init_config, instances)

        self._server = self.instance.get('server', '')
        self._port = self.instance.get('port')
        self._db = self.instance.get('db', 'default')
        self._user = self.instance.get('user', 'default')
        self._password = self.instance.get('password', '')
        self._connect_timeout = float(self.instance.get('connect_timeout', 10))
        self._read_timeout = float(self.instance.get('read_timeout', 10))
        self._ping_timeout = float(self.instance.get('ping_timeout', 5))
        self._compression = self.instance.get('compression', False)
        self._tls_verify = is_affirmative(self.instance.get('tls_verify', False))
        self._tags = self.instance.get('tags', [])

        # Add global tags
        self._tags.append('server:{}'.format(self._server))
        self._tags.append('port:{}'.format(self._port))
        self._tags.append('db:{}'.format(self._db))

        custom_queries = self.instance.get('custom_queries', [])
        use_global_custom_queries = self.instance.get('use_global_custom_queries', True)

        # Handle overrides
        if use_global_custom_queries == 'extend':
            custom_queries.extend(self.init_config.get('global_custom_queries', []))
        elif 'global_custom_queries' in self.init_config and is_affirmative(use_global_custom_queries):
            custom_queries = self.init_config.get('global_custom_queries', [])

        # Deduplicate
        self._custom_queries = list(iter_unique(custom_queries))

        # We'll connect on the first check run
        self._client = None
        self.check_initializations.append(self.create_connection)

        self._error_sanitizer = ErrorSanitizer(self._password)

        self._collection_methods = (self.query_system_metrics, self.query_system_events)

    def check(self, _):
        for collection_method in self._collection_methods:
            try:
                collection_method()
            except QueryExecutionError as e:
                self.log.error('Error querying %s: %s', e.source, e)
                continue
            except Exception as e:
                self.log.error('Unexpected error running `%s`: %s', collection_method.__name__, e)
                continue

    def query_system_metrics(self):
        # https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-metrics
        self.execute_query(queries.SystemMetrics)

    def query_system_events(self):
        # https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-events
        self.execute_query(queries.SystemEvents)

    def query_custom(self):
        for custom_query in self._custom_queries:
            query = custom_query.get('query')
            if not query:  # no cov
                self.log.error('Custom query field `query` is required')
                continue

            columns = custom_query.get('columns')
            if not columns:  # no cov
                self.log.error('Custom query field `columns` is required')
                continue

            self.log.debug('Running custom query for SAP HANA')
            rows = self.iter_rows_raw(query)

            # Trigger query execution
            try:
                first_row = next(rows)
            except Exception as e:  # no cov
                self.log.error('Error executing custom query: {}'.format(e))
                continue

            for row in chain((first_row,), rows):
                if not row:  # no cov
                    self.log.debug('Custom query returned an empty result')
                    continue

                if len(columns) != len(row):  # no cov
                    self.log.error('Custom query result expected {} column(s), got {}'.format(len(columns), len(row)))
                    continue

                metric_info = []
                query_tags = list(self._tags)
                query_tags.extend(custom_query.get('tags', []))

                for column, value in zip(columns, row):
                    # Columns can be ignored via configuration.
                    if not column:  # no cov
                        continue

                    name = column.get('name')
                    if not name:  # no cov
                        self.log.error('Column field `name` is required')
                        break

                    column_type = column.get('type')
                    if not column_type:  # no cov
                        self.log.error('Column field `type` is required for column `{}`'.format(name))
                        break

                    if column_type == 'tag':
                        query_tags.append('{}:{}'.format(name, value))
                    else:
                        if not hasattr(self, column_type):
                            self.log.error(
                                'Invalid submission method `{}` for metric column `{}`'.format(column_type, name)
                            )
                            break
                        try:
                            metric_info.append((name, float(value), column_type))
                        except (ValueError, TypeError):  # no cov
                            self.log.error('Non-numeric value `{}` for metric column `{}`'.format(value, name))
                            break

                # Only submit metrics if there were absolutely no errors - all or nothing.
                else:
                    for info in metric_info:
                        metric, value, method = info
                        getattr(self, method)(metric, value, tags=query_tags)

    def execute_query(self, query):
        # Re-use column access map for efficiency
        result = {}

        # Avoid repeated global lookups
        get_method = getattr

        try:
            rows = self._client.execute_iter(query.query)
            for (column, metric_data), value in zip(query.fields, rows):
                value = value[0]

                for metric, method in metric_data:
                    if metric:
                        get_method(self, method)(metric, value, tags=self._tags)

                result[column] = value
        except Exception as e:
            raise QueryExecutionError(self._error_sanitizer.clean(str(e)), ', '.join(sorted(query.views)))

        # Return all values for possible post-processing
        return result

    def iter_rows_raw(self, query):
        try:
            rows = self._client.execute_iter(query.query)
            for row in rows:
                yield row
        except Exception as e:
            raise QueryExecutionError(self._error_sanitizer.clean(str(e)), 'custom query')

    def create_connection(self):
        try:
            client = clickhouse_driver.Client(
                host=self._server,
                port=self._port,
                database=self._db,
                user=self._user,
                password=self._password,
                connect_timeout=self._connect_timeout,
                send_receive_timeout=self._read_timeout,
                sync_request_timeout=self._ping_timeout,
                compression=self._compression,
                verify=self._tls_verify,
                # Don't pollute the Agent logs
                settings={'calculate_text_stack_trace': False},
                # Make every client unique for server logs
                client_name='datadog-{}'.format(self.check_id),
            )
            client.connection.connect()
        except Exception as e:
            error = 'Unable to connect to ClickHouse: {}'.format(
                self._error_sanitizer.clean(self._error_sanitizer.scrub(str(e)))
            )
            self.service_check(self.SERVICE_CHECK_CONNECT, self.CRITICAL, message=error, tags=self._tags)

            raise_from(type(e)(error), None)
        else:
            self.service_check(self.SERVICE_CHECK_CONNECT, self.OK, tags=self._tags)
            self._client = client
