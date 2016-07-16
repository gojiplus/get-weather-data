#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import multiprocessing
import logging
import Queue

from ConfigParser import ConfigParser
from multiprocessing.managers import SyncManager

from zip2wd import WeatherByZip

from pkg_resources import resource_filename

CONFIG_FILE_NAME = 'zip2wd.cfg'
DEFAULT_CONFIG_FILE = resource_filename(__name__, CONFIG_FILE_NAME)
LOG_FILE = 'zip2wd_worker.log'

DEFAULT_ZIP2WS_DB_FILE = resource_filename('zip2ws', 'data/zip2ws.sqlite')


def setup_logger(debug):
    """ Set up logging
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=LOG_FILE,
                        filemode='w')
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def parse_command_line():
    """Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Weather search by ZIP"
                                     " (Worker)")

    parser.add_argument("--config", type=str, dest="config",
                        default=DEFAULT_CONFIG_FILE,
                        help="Default configuration file"
                        " (default: {0!s})".format(DEFAULT_CONFIG_FILE))
    parser.add_argument('-v', '--verbose', dest='verbose',
                        action='store_true',
                        help="Verbose message")
    parser.set_defaults(verbose=False)

    return parser.parse_args()


def load_config(args=None):
    if args is None or isinstance(args, basestring):
        namespace = argparse.Namespace()
        if args is None:
            if os.path.exists(CONFIG_FILE_NAME):
                namespace.config = CONFIG_FILE_NAME
            else:
                namespace.config = DEFAULT_CONFIG_FILE
        else:
            namespace.config = args
        args = namespace
    try:
        config = ConfigParser()
        config.read(args.config)
        args.ip = config.get('manager', 'ip')
        args.port = config.getint('manager', 'port')
        args.authkey = config.get('manager', 'authkey')
        args.batch_size = config.getint('manager', 'batch_size')
        args.columns = config.get('output', 'columns')

        args.uses_sqlite = config.getboolean('worker', 'uses_sqlite')
        args.processes = config.getint('worker', 'processes')
        args.nth = config.getint('worker', 'nth')
        args.distance = config.getint('worker', 'distance')

        args.dbpath = config.get('db', 'path')
        path = os.path.join(args.dbpath, config.get('db', 'zip2ws'))
        if not os.path.exists(path):
            logging.warn("ZIP2WS database '{:s}' is not exists".format(path))
            path = DEFAULT_ZIP2WS_DB_FILE
            logging.warn("Using default from '{:s}'".format(path))
        args.zip2ws_db = path
    except Exception as e:
        logging.error(str(e))

    return args


def zip2wd_worker(job_q, result_q, args):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """
    weather = WeatherByZip(args)
    while True:
        try:
            job = job_q.get_nowait()
            outdict = {}
            for i, j in enumerate(job):
                outdict[i] = weather.search(j)
            result_q.put(outdict)
        except Queue.Empty:
            return


def mp_zip2wd(shared_job_q, shared_result_q, args):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        factorizer_worker as the worker function, and wait until all are
        finished.
    """
    procs = []
    for i in range(args.processes):
        p = multiprocessing.Process(target=zip2wd_worker,
                                    args=(shared_job_q, shared_result_q, args))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


def make_worker_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(SyncManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    logging.info("Worker connected to {:s}:{:d}".format(ip, port))
    return manager


def run_worker(args):
    manager = make_worker_manager(args.ip, args.port, args.authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    mp_zip2wd(job_q, result_q, args)


def main(args=None):
    if args is None:
        args = parse_command_line()

    setup_logger(args.verbose)

    args = load_config(args)
    logging.info(str(args))

    run_worker(args)

if __name__ == "__main__":
    main()
