#!/usr/bin/env python

import logging
import sys
import time
from typing import Dict, List

from configargparse import ArgumentParser
from prettylog import basic_config, LogFormat
from pyrabbit2.api import Client
from pyrabbit2.http import NetworkError

parser = ArgumentParser(auto_env_var_prefix="RQMB_")

parser.add_argument("--api-url", type=str, default="localhost:15672")

parser.add_argument("--user", type=str, default="guest")

parser.add_argument("--password", type=str, default="guest")

parser.add_argument(
    "-L",
    "--log-level",
    help="Log level",
    default="info",
    choices=(
        "critical",
        "fatal",
        "error",
        "warning",
        "warn",
        "info",
        "debug",
        "notset",
    ),
)

parser.add_argument(
    "--log-format", choices=LogFormat.choices(), default=LogFormat.stream
)

parser.add_argument(
    "--policy-name",
    type=str,
    default="queue_master_balancer",
    help="Policy name for relocate queues",
)

parser.add_argument(
    "--dry-run",
    action="store_true",
    help=(
        "Show how many queues will move from one node (with a maximum queues)"
        "to another (with a minimum queues)"
    ),
)

parser.add_argument(
    "--queue-delta",
    type=int,
    default=3,
    help=(
        "Reasonable delta of queues between max and min numbers of queues"
        "on node when script do nothing"
    ),
)

parser.add_argument(
    "--sleep-time",
    type=int,
    default=20,
    help="Seconds for sleep between queues balancing",
)

parser.add_argument(
    "--sleep-time-action",
    type=int,
    default=3,
    help="Seconds for sleep between RabbitMQ API requests sending",
)

arguments = parser.parse_args()

log = logging.getLogger()


def wait_for_client(client) -> bool:
    """
    :param client:
    :return: bool
    """

    log.debug("RabbitMQ http not ready, waiting...")
    try:
        return client.is_alive()
    except NetworkError:
        time.sleep(5)
        wait_for_client(client)


def nodes_dict(nodes_info_data, vhost_names: list) -> Dict[str, List]:
    """
    :param client
    :param vhost_names list of vhosts
    :return: dict: Key is a name of rabbit node, Value is empty list
    """
    temp_dict = dict.fromkeys((vhost for vhost in vhost_names))

    nodes_dictionary = dict.fromkeys(
        (node["name"] for node in nodes_info_data), temp_dict
    )
    return nodes_dictionary


def master_nodes_queues(
    nodes_dict: dict, vhost_names: list
) -> Dict[str, Dict[str, List]]:
    """
    :param queues_data: list of dicts with all queues info
    :param vhost_names list of vhosts
    :return: dict {node_name: {vhost1: list_queues, vhost2: list_queues}
    """

    master_nodes_queues_dict = {}

    queues_data = client.get_queues()
    for node in nodes_dict.keys():

        vhost_dict = {}

        for vhost in vhost_names:
            list_queues = []

            for queue in queues_data:
                if node == queue["node"] and queue["vhost"] == vhost:
                    list_queues.append(queue["name"])

            vhost_dict[vhost] = list_queues
            master_nodes_queues_dict[node] = vhost_dict

    return master_nodes_queues_dict


def calculate_queues(master_nodes_queues_dict: dict) -> Dict[str, int]:
    """
    :param master_nodes_queues_dict: dict
    :return: dict {node1: number_queues, node2: number_queues,}
    """

    calculated_dict = {}

    for node, vhost in master_nodes_queues_dict.items():
        counter = sum(map(len, vhost.values()))
        calculated_dict[node] = counter

    return calculated_dict


def calculate_vhost(max_master_dict: dict) -> Dict[str, int]:
    """
    :param max_master_dict:
    :return: dict {vhost, number_queues}
    """
    calculated_dict = {}

    for vhost, queues in max_master_dict.items():
        if not queues:
            continue
        calculated_dict[vhost] = max(map(len, queues))
    return calculated_dict


def is_relocate(max_queues: int, min_queues: int, queue_delta: int) -> bool:
    """
    :param min_queues: int
    :param max_queues: int
    :return: bool
    """
    return max_queues - min_queues > queue_delta


def relocate_policy(
    queue_for_relocate: str,
    max_queues_vhost: str,
    min_queues_node: str,
    policy_name: str,
):

    definition_dict = {
        "ha-mode": "nodes",
        "ha-params": min_queues_node.split(" "),
    }
    dict_params = {
        "pattern": queue_for_relocate,
        "definition": definition_dict,
        "priority": 999,
        "apply-to": "queues",
    }
    log.debug("Policy body dict is %r", dict_params)

    client.create_policy(
        vhost=max_queues_vhost, policy_name=policy_name, **dict_params
    )


def is_queue_running(client, vhost: str, queue: str) -> bool:
    state = None
    while state != "running":
        try:
            state = client.get_queue(vhost, queue)["state"]
            log.debug("Queue %r has state %r", queue, state)
            if state != "running":
                time.sleep(1)
            else:
                return True
        except KeyError:
            log.exception("RabbitMQ API not ready to answer")


def relocate_queue(
    client,
    max_master_dict: dict,
    max_queues_vhost: str,
    min_queues_node: str,
    sleep_time: int,
):

    queue_for_relocate = max_master_dict[max_queues_vhost][0]
    log.debug("Queue for relocate is %r", queue_for_relocate)
    log.debug("Creating relocate policy")
    relocate_policy(
        queue_for_relocate,
        max_queues_vhost,
        min_queues_node,
        arguments.policy_name,
    )
    time.sleep(sleep_time)
    client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
    time.sleep(sleep_time)
    is_queue_running(client, max_queues_vhost, queue_for_relocate)
    client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
    time.sleep(sleep_time)
    client.delete_policy(max_queues_vhost, arguments.policy_name)
    time.sleep(sleep_time)
    client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
    time.sleep(sleep_time)
    is_queue_running(client, max_queues_vhost, queue_for_relocate)


if __name__ == "__main__":
    basic_config(
        level=arguments.log_level.upper(),
        buffered=False,
        log_format=arguments.log_format,
        date_format=True,
    )

    client = Client(arguments.api_url, arguments.user, arguments.password)

    wait_for_client(client)
    log.debug("RabbitMQ alive")

    while True:
        nodes_info_data = client.get_nodes()

        log.debug("Nodes info: %r", nodes_info_data)

        vhost_names = client.get_vhost_names()
        log.debug("Vhost names: %r", vhost_names)

        master_nodes_queues_dict = master_nodes_queues(
            nodes_dict(nodes_info_data, vhost_names), vhost_names
        )
        log.debug("Master nodes info: %r", master_nodes_queues_dict)

        calculated_queues = calculate_queues(master_nodes_queues_dict)

        min_queues_node = min(calculated_queues, key=calculated_queues.get)
        max_queues_node = max(calculated_queues, key=calculated_queues.get)

        min_queues = calculated_queues.get(min_queues_node)
        log.debug("Min number of queues is %r", min_queues)
        max_queues = calculated_queues.get(max_queues_node)
        log.debug("Max number of queues is %r", max_queues)

        max_master_dict = master_nodes_queues_dict[max_queues_node]
        calculated_vhost = calculate_vhost(max_master_dict)
        max_queues_vhost = max(calculated_vhost, key=calculated_vhost.get)

        if arguments.dry_run:
            log.info("It's a dry run. Calculate queues...")
            for node, queue_number in calculated_queues.items():
                log.info("Node %r is a master of %r queues", node, queue_number)
            sys.exit(0)

        if is_relocate(max_queues, min_queues, arguments.queue_delta):
            relocate_queue(
                client,
                max_master_dict,
                max_queues_vhost,
                min_queues_node,
                sleep_time=arguments.sleep_time_action,
            )
            log.debug(
                "Queue balancer sleeping for %r seconds", arguments.sleep_time
            )
            time.sleep(arguments.sleep_time)
        else:
            log.info("Nothing to do")
