#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import logging
import os
import time
from configparser import ConfigParser
from functools import partial
from importlib.resources import files
from logging.handlers import RotatingFileHandler
from multiprocessing.managers import SyncManager
from queue import Queue as _Queue
from typing import Any

import click
from rich.console import Console

from zip2wd import STATION_INFO_COLS

CONFIG_FILE_NAME = "zip2wd.cfg"
DEFAULT_CONFIG_FILE = str(files(__package__) / CONFIG_FILE_NAME)
DEF_OUTPUT_FILE = "output.csv"
LOG_FILE = "zip2wd_manager.log"

console = Console()


def setup_logging(verbose: bool = False, log_file: str = LOG_FILE) -> None:
    """Set up logging with console and file handlers."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3),
        ],
    )


class Queue(_Queue):  # type: ignore[type-arg]
    """A picklable queue."""

    def __getstate__(self) -> tuple[int, Any, int]:
        return (self.maxsize, self.queue, self.unfinished_tasks)

    def __setstate__(self, state: tuple[int, Any, int]) -> None:
        Queue.__init__(self)
        self.maxsize = state[0]
        self.queue = state[1]
        self.unfinished_tasks = state[2]


def get_q(q: Queue) -> Queue:
    """Return the queue."""
    return q


class JobQueueManager(SyncManager):
    """Manager for job queues."""

    pass


def make_server_manager(ip: str, port: int, authkey: bytes) -> JobQueueManager:
    """Create a manager for the server, listening on the given port."""
    job_q: Queue = Queue()
    result_q: Queue = Queue()

    JobQueueManager.register("get_job_q", callable=partial(get_q, job_q))
    JobQueueManager.register("get_result_q", callable=partial(get_q, result_q))

    manager = JobQueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    logging.info(f"Manager started at port {port:d}")
    return manager


def load_config(config_file: str) -> dict[str, Any]:
    """Load configuration from file."""
    config = ConfigParser()
    config.read(config_file)

    return {
        "ip": config.get("manager", "ip"),
        "port": config.getint("manager", "port"),
        "authkey": config.get("manager", "authkey").encode(),
        "batch_size": config.getint("manager", "batch_size"),
        "columns": config.get("output", "columns"),
    }


def run_manager(
    inputs: tuple[str, ...],
    outfile: str,
    config: dict[str, Any],
) -> None:
    """Run the manager."""
    manager = make_server_manager(config["ip"], config["port"], config["authkey"])
    shared_job_q = manager.get_job_q()  # type: ignore[attr-defined]
    shared_result_q = manager.get_result_q()  # type: ignore[attr-defined]

    columns_file = config["columns"]
    try:
        with open(columns_file, "r", encoding="utf-8") as f:
            output_columns = [r.strip() for r in f.readlines() if r[0] != "#"]
    except FileNotFoundError:
        columns_path = str(files(__package__) / config["columns"])
        with open(columns_path, "r", encoding="utf-8") as f:
            output_columns = [r.strip() for r in f.readlines() if r[0] != "#"]

    outfile_handle = open(outfile, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(
        outfile_handle,
        fieldnames=["uniqid", "zip", "year", "month", "day"]
        + STATION_INFO_COLS
        + output_columns,
    )
    writer.writeheader()

    for infile in inputs:
        with open(infile, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames and "from.day" in reader.fieldnames:
                extended = True
            else:
                extended = False
            zips: list[dict[str, Any]] = []
            for r in reader:
                data: dict[str, Any] = {}
                data["uniqid"] = r["uniqid"]
                data["zip"] = r["zip"]
                if extended:
                    data["from.year"] = int(r["from.year"])
                    data["from.month"] = int(r["from.month"])
                    data["from.day"] = int(r["from.day"])
                    data["to.year"] = int(r["to.year"])
                    data["to.month"] = int(r["to.month"])
                    data["to.day"] = int(r["to.day"])
                else:
                    data["from.year"] = int(r["year"])
                    data["from.month"] = int(r["month"])
                    data["from.day"] = int(r["day"])
                    data["to.year"] = int(r["year"])
                    data["to.month"] = int(r["month"])
                    data["to.day"] = int(r["day"])
                zips.append(data)
        N = len(zips)
        logging.info(f"Processing: '{infile:s}', total ZIP = {N:d}")

        chunksize = config["batch_size"]
        for i in range(0, len(zips), chunksize):
            shared_job_q.put(zips[i : i + chunksize])

        numresults = 0
        while numresults < N:
            try:
                if not shared_result_q.empty():
                    outdict = shared_result_q.get()
                    for v in outdict.values():
                        writer.writerows(v)
                    numresults += len(outdict)
                    logging.info(f"Progress: {numresults:d}/{N:d}")
                else:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                break
    time.sleep(3)
    manager.shutdown()
    outfile_handle.close()


@click.command()
@click.argument("inputs", nargs=-1, required=True)
@click.option(
    "--config",
    default=DEFAULT_CONFIG_FILE,
    help=f"Configuration file (default: {DEFAULT_CONFIG_FILE})",
)
@click.option(
    "-o",
    "--out",
    default=DEF_OUTPUT_FILE,
    help=f"Output CSV file (default: {DEF_OUTPUT_FILE})",
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
def cli(inputs: tuple[str, ...], config: str, out: str, verbose: bool) -> None:
    """Weather search by ZIP (Manager)."""
    setup_logging(verbose)
    console.print("[bold]Starting manager...[/bold]")

    if not os.path.exists(config):
        console.print(f"[red]Config file not found: {config}[/red]")
        raise SystemExit(1)

    cfg = load_config(config)
    logging.info(f"Config: {cfg}")

    run_manager(inputs, out, cfg)
    console.print("[green]Manager completed.[/green]")


def main() -> None:
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
