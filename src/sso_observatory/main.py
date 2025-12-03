"""Entrypoint for the SSOObservatory ingestion job."""
from __future__ import annotations

import logging
import sys

from .config import get_config
from .data_collector import EnterpriseAppCollector
from .db import DatabaseClient
from .graph_client import GraphClient


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )


def run() -> None:
    configure_logging()
    config = get_config()
    graph_client = GraphClient(config.azure)
    database = DatabaseClient(config.database)
    collector = EnterpriseAppCollector(config, graph_client, database)
    try:
        collector.run()
    finally:
        database.close()


if __name__ == "__main__":
    run()
