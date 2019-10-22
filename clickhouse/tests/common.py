# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import os

from datadog_checks.dev import get_docker_hostname, get_here

HERE = get_here()
COMPOSE_FILE = os.path.join(HERE, 'docker', 'docker-compose.yaml')

HOST = get_docker_hostname()
HTTP_PORT = 8123
TCP_PORT = 9000

CONFIG = {
    'server': HOST,
    'port': TCP_PORT,
    'user': 'datadog',
    'password': 'Datadog123!',
    'tags': ['foo:bar'],
}

# TODO: Remove
E2E_METADATA = {'start_commands': ['pip install clickhouse-driver']}
