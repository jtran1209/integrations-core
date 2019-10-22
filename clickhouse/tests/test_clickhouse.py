# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from datadog_checks.clickhouse import ClickhouseCheck


def test_check(aggregator, instance):
    check = ClickhouseCheck('clickhouse', {}, [{}])
    check.check(instance)

    aggregator.assert_all_metrics_covered()
