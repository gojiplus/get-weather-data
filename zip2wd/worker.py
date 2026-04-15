#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import multiprocessing
import os
import queue
from configparser import ConfigParser
from importlib.resources import files
from logging.handlers import RotatingFileHandler
from multiprocessing.managers import SyncManager
from typing import Any

import click
from rich.console import Console

from zip2wd import WeatherByZip

CONFIG_FILE_NAME = "zip2wd.cfg"
DEFAULT_CONFIG_FILE = str(files(__package__) / CONFIG_FILE_NAME)
LOG_FILE = "zip2wd_worker.log"

DEFAULT_ZIP2WS_DB_FILE = str(files("zip2ws") / "data" / "zip2ws.sqlite")

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


class WorkerArgs:
    """Worker configuration arguments."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.uses_sqlite = config["uses_sqlite"]
        self.processes = config["processes"]
        self.nth = config["nth"]
        self.distance = config["distance"]
        self.columns = config["columns"]
        self.dbpath = config["dbpath"]
        self.zip2ws_db = config["zip2ws_db"]


def load_config(config_file: str) -> dict[str, Any]:
    """Load configuration from file."""
    config = ConfigParser()
    config.read(config_file)

    dbpath = config.get("db", "path")
    zip2ws_path = os.path.join(dbpath, config.get("db", "zip2ws"))
    if not os.path.exists(zip2ws_path):
        logging.warning(f"ZIP2WS database '{zip2ws_path}' does not exist")
        zip2ws_path = DEFAULT_ZIP2WS_DB_FILE
        logging.warning(f"Using default from '{zip2ws_path}'")

    return {
        "ip": config.get("manager", "ip"),
        "port": config.getint("manager", "port"),
        "authkey": config.get("manager", "authkey").encode(),
        "batch_size": config.getint("manager", "batch_size"),
        "columns": config.get("output", "columns"),
        "uses_sqlite": config.getboolean("worker", "uses_sqlite"),
        "processes": config.getint("worker", "processes"),
        "nth": config.getint("worker", "nth"),
        "distance": config.getint("worker", "distance"),
        "dbpath": dbpath,
        "zip2ws_db": zip2ws_path,
    }


def zip2wd_worker(job_q: Any, result_q: Any, args: WorkerArgs) -> None:
    """Worker function to be launched in a separate process."""
    weather = WeatherByZip(args)
    while True:
        try:
            job = job_q.get_nowait()
            outdict: dict[int, list[dict[str, Any]]] = {}
            for idx, j in enumerate(job):
                outdict[idx] = weather.search(j)
            result_q.put(outdict)
        except queue.Empty:
            return


def mp_zip2wd(shared_job_q: Any, shared_result_q: Any, args: WorkerArgs) -> None:
    """Split the work with jobs in shared_job_q and results in shared_result_q."""
    procs = []
    for _ in range(args.processes):
        p = multiprocessing.Process(
            target=zip2wd_worker, args=(shared_job_q, shared_result_q, args)
        )
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


def make_worker_manager(ip: str, port: int, authkey: bytes) -> SyncManager:
    """Create a manager for a client."""

    class ServerQueueManager(SyncManager):
        pass

    ServerQueueManager.register("get_job_q")
    ServerQueueManager.register("get_result_q")

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    logging.info(f"Worker connected to {ip:s}:{port:d}")
    return manager


def run_worker(config: dict[str, Any]) -> None:
    """Run the worker."""
    manager = make_worker_manager(config["ip"], config["port"], config["authkey"])
    job_q = manager.get_job_q()  # type: ignore[attr-defined]
    result_q = manager.get_result_q()  # type: ignore[attr-defined]
    args = WorkerArgs(config)
    mp_zip2wd(job_q, result_q, args)


@click.command()
@click.option(
    "--config",
    default=DEFAULT_CONFIG_FILE,
    help=f"Configuration file (default: {DEFAULT_CONFIG_FILE})",
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
def cli(config: str, verbose: bool) -> None:
    """Weather search by ZIP (Worker)."""
    setup_logging(verbose)
    console.print("[bold]Starting worker...[/bold]")

    if not os.path.exists(config):
        console.print(f"[red]Config file not found: {config}[/red]")
        raise SystemExit(1)

    cfg = load_config(config)
    logging.info(f"Config: {cfg}")

    run_worker(cfg)
    console.print("[green]Worker completed.[/green]")


def main() -> None:
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
