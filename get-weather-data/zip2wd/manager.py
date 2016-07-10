#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import time
import csv

from ConfigParser import ConfigParser
from functools import partial
from Queue import Queue as _Queue

from multiprocessing.managers import SyncManager

from zip2wd import STATION_INFO_COLS

from pkg_resources import resource_filename

CONFIG_FILE_NAME = 'zip2wd.cfg'
DEFAULT_CONFIG_FILE = resource_filename(__name__, CONFIG_FILE_NAME)
DEF_OUTPUT_FILE = 'output.csv'
LOG_FILE = 'zip2wd_manager.log'


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
                                     " (Manager)")

    parser.add_argument('inputs', nargs='+', help='CSV input file(s) name')
    parser.add_argument("--config", type=str, dest="config",
                        default=DEFAULT_CONFIG_FILE,
                        help="Default configuration file"
                        " (default: {0!s})".format(DEFAULT_CONFIG_FILE))
    parser.add_argument("-o", "--out", type=str, dest="outfile",
                        default=DEF_OUTPUT_FILE,
                        help="Search results in CSV (default: {0:s})"
                        .format(DEF_OUTPUT_FILE))
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
    except Exception as e:
        logging.error(str(e))

    return args


class Queue(_Queue):
    """ A picklable queue. """
    def __getstate__(self):
        # Only pickle the state we care about
        return (self.maxsize, self.queue, self.unfinished_tasks)

    def __setstate__(self, state):
        # Re-initialize the object, then overwrite the default state with
        # our pickled state.
        Queue.__init__(self)
        self.maxsize = state[0]
        self.queue = state[1]
        self.unfinished_tasks = state[2]


def get_q(q):
    return q

class JobQueueManager(SyncManager):
    pass


def make_server_manager(ip, port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """

    job_q = Queue()
    result_q = Queue()

    JobQueueManager.register('get_job_q', callable=partial(get_q, job_q))
    JobQueueManager.register('get_result_q', callable=partial(get_q, result_q))

    manager = JobQueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    logging.info('Manager started at port {:d}'.format(port))
    return manager


def run_manager(args):
    # Start a shared manager server and access its queues
    manager = make_server_manager(args.ip, args.port, args.authkey)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    try:
        with open(args.columns, 'rb') as f:
            output_columns = [r.strip() for r in f.readlines() if r[0] != '#']
    except:
        args.columns = resource_filename(__name__, args.columns)
        with open(args.columns, 'rb') as f:
            output_columns = [r.strip() for r in f.readlines() if r[0] != '#']

    outfile = open(args.outfile, 'wb')
    writer = csv.DictWriter(outfile, fieldnames=['uniqid', 'zip', 'year',
                            'month', 'day'] + STATION_INFO_COLS +
                            output_columns)
    writer.writeheader()
    for infile in args.inputs:
        with open(infile, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            if 'from.day' in reader.fieldnames:
                args.extended = True
            else:
                args.extended = False
            zips = []
            for r in reader:
                data = {}
                data['uniqid'] = r['uniqid']
                data['zip'] = r['zip']
                if args.extended:
                    data['from.year'] = int(r['from.year'])
                    data['from.month'] = int(r['from.month'])
                    data['from.day'] = int(r['from.day'])
                    data['to.year'] = int(r['to.year'])
                    data['to.month'] = int(r['to.month'])
                    data['to.day'] = int(r['to.day'])
                else:
                    data['from.year'] = int(r['year'])
                    data['from.month'] = int(r['month'])
                    data['from.day'] = int(r['day'])
                    data['to.year'] = int(r['year'])
                    data['to.month'] = int(r['month'])
                    data['to.day'] = int(r['day'])
                zips.append(data)
        N = len(zips)
        logging.info("Processing: '{:s}', total ZIP = {:d}".format(infile, N))

        # The zips are split into chunks. Each chunk is pushed into the job
        # queue.
        chunksize = args.batch_size
        for i in range(0, len(zips), chunksize):
            shared_job_q.put(zips[i:i + chunksize])

        # Wait until all results are ready in shared_result_q
        numresults = 0
        while numresults < N:
            try:
                if not shared_result_q.empty():
                    outdict = shared_result_q.get()
                    for v in outdict.values():
                        writer.writerows(v)
                    numresults += len(outdict)
                    logging.info("Progress: {:d}/{:d}".format(numresults, N))
                else:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                break
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(3)
    manager.shutdown()
    outfile.close()


def main(args=None):
    if args is None:
        args = parse_command_line()

    setup_logger(args.verbose)

    args = load_config(args)
    logging.info(str(args))

    run_manager(args)

if __name__ == "__main__":
    main()
