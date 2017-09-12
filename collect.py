#!/usr/bin/env python

"""
This is a rough collector script for running components on a host and storing
their output if possible.
"""

import logging
import multiprocessing as mp
import os
import platform
from contextlib import closing

from insights import specs  # noqa: F401
from insights.core import dr, plugins
from insights.core.context import HostContext
from insights.core.serde import persister
from insights.util import fs

log = logging.getLogger(__name__)


def worker(args):
    try:
        return run_graph(*args)
    except Exception as ex:
        log.exception(ex)


def run_graph(seed_broker, g, output_dir):
    to_save = [plugins.datasource, plugins.parser, plugins.combiner, plugins.rule]
    broker = dr.Broker(seed_broker)
    for _type in to_save:
        path = os.path.join(output_dir, dr.get_simple_name(_type))
        fs.ensure_path(path)
        broker.add_observer(persister(path), _type)
    dr.run(g, broker)


def run_parallel(args):
    num_procs = max((mp.cpu_count() / 2), 2)
    log.info("Collector running with %s processes." % num_procs)
    with closing(mp.Pool(num_procs)) as pool:
        pool.map(worker, args)


def run_serial(args):
    log.info("Collector running serially.")
    for a in args:
        worker(a)


def main():
    hostname = platform.node()
    ctx = HostContext(hostname)

    broker = dr.Broker()
    broker[HostContext] = ctx

    out_path = "output"
    dr.load_components("insights/parsers")
    dr.load_components("insights/combiners")

    graphs = dr.get_subgraphs(dr.COMPONENTS[dr.GROUPS.single])
    args = [(broker, g, out_path) for g in graphs]
    run_serial(args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
