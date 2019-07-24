#!/usr/bin/env python

import logging
import time

from configargparse import ArgumentParser
from prettylog import basic_config, LogFormat
from pyrabbit2.api import Client
from pyrabbit2.http import NetworkError

parser = ArgumentParser(auto_env_var_prefix='RQMB_')

parser.add_argument(
    '--api-url',
    type=str,
    default='localhost:15672'
)

parser.add_argument(
    '--user',
    type=str,
    default='guest'
)

parser.add_argument(
    '--password',
    type=str,
    default='guest'
)

parser.add_argument(
    '-L', '--log-level', help='Log level',
    default='info',
    choices=(
        'critical', 'fatal', 'error', 'warning',
        'warn', 'info', 'debug', 'notset'
    ),
)

parser.add_argument(
    '--log-format', choices=LogFormat.choices(), default=LogFormat.stream
)

parser.add_argument(
    '--dry-run',
    action="store_true",
    help='Show how many queues will move from one node (with a maximum queues) \
    to another (with a minimum queues)'
)

parser.add_argument(
    '--queue-delta',
    type=int,
    default=3,
    help='Reasonable delta of queues between max and min numbers of queues \
    on node when script do nothing'
)

parser.add_argument(
    '--relocate-queues',
    type=int,
    default=2,
    help='How many queues will relocate in one time'
)

arguments = parser.parse_args()

log = logging.getLogger()


def wait_for_client(client):
    '''
    :param client:
    :return: bool
    '''

    log.debug('RabbitMQ http not ready, waiting...')
    try:
        return client.is_alive()
    except NetworkError:
        time.sleep(5)
        wait_for_client(client)


def nodes_dict(nodes_info_data, vhost_names: list):
    '''
    :param client
    :param vhost_names list of vhosts
    :return: dict: Key is a name of rabbit node, Value is empty list
    '''
    nodes_dictionary = dict.fromkeys(
        (node['name'] for node in nodes_info_data), dict.fromkeys(
            (vhost for vhost in vhost_names)
        )
    )
    return nodes_dictionary


def master_nodes_queues(nodes_dict: dict, vhost_names: list):
    '''
    :param queues_data: list of dicts with all queues info
    :param vhost_names list of vhosts
    :return: dict {node_name: {vhost1: list_queues, vhost2: list_queues}
    '''

    master_nodes_queues_dict = {}

    queues_data = client.get_queues()
    for node in nodes_dict.items():

        vhost_dict = {}

        for vhost in vhost_names:
            list_queues = []

            for queue in queues_data:
                if node[0] == queue['node'] and queue['vhost'] == vhost:
                    list_queues.append(queue['name'])

            vhost_dict.update({vhost: list_queues})

            master_nodes_queues_dict.update({node[0]: vhost_dict})

    return master_nodes_queues_dict


def calculate_queues(master_nodes_queues_dict: dict):
    '''
    :param master_nodes_queues_dict: dict
    :return: dict {node1: number_queues, node2: number_queues,}
    '''

    calculated_dict = {}

    for node in master_nodes_queues_dict.items():
        counter = 0
        for i in node[1].values():
            counter += len(i)
        calculated_dict.update({node[0]: counter})
    return calculated_dict


def is_relocate(max_queues: int, min_queues: int):
    '''
    :param min_queues: int
    :param max_queues: int
    :return: bool
    '''
    if max_queues - min_queues > arguments.queue_delta:
        return True


def relocate():
    pass
    # TODO


def check_relocate_status():
    pass
    # TODO


if __name__ == '__main__':
    basic_config(
        level=arguments.log_level.upper(),
        buffered=False,
        log_format=arguments.log_format,
        date_format=True
    )

    client = Client(arguments.api_url, arguments.user, arguments.password)

    wait_for_client(client)
    log.debug('RabbitMQ alive')

    nodes_info_data = client.get_nodes()

    log.debug('{} {}'.format('Nodes info: ', nodes_info_data))

    vhost_names = client.get_vhost_names()
    log.debug('{} {}'.format('Vhost names: ', vhost_names))

    master_nodes_queues_dict = master_nodes_queues(
        nodes_dict(nodes_info_data, vhost_names), vhost_names
    )
    log.debug('{} {}'.format('Master nodes info: ', master_nodes_queues_dict))

    calculated_queues = calculate_queues(master_nodes_queues_dict)

    min_queues_node = min(calculated_queues, key=calculated_queues.get)
    max_queues_node = max(calculated_queues, key=calculated_queues.get)
    min_queues = calculated_queues.get(min_queues_node)
    max_queues = calculated_queues.get(max_queues_node)

    if is_relocate(max_queues, min_queues):
        if not arguments.dry_run:
            pass
            # TODO: call def relocate
        else:
            for node in calculated_queues.items():
                log.info(
                    'Node {} is a master of {} queues'.format(node[0], node[1])
                )
            log.info('It\'s a dry run. You need relocate from {} to {} {} \
                     queues'.format(
                max_queues_node,
                min_queues_node,
                arguments.relocate_queues
            ))
