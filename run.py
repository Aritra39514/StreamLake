"""
run.py — single entry point for all StreamLake components.

Usage
─────
  python run.py api                        start the control plane on :8000
  python run.py producer [--rate N]        start the fake order producer
  python run.py query <table_name>         scan latest rows from an Iceberg table
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-28s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)


def run_api():
    import uvicorn
    from streamlake.config import settings

    uvicorn.run(
        "streamlake.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )


def run_producer(argv):
    sys.argv = ["order_producer"] + argv
    from producer.order_producer import main
    main()


def run_query(table_name: str, limit: int):
    from streamlake.iceberg_writer import IcebergWriter

    writer = IcebergWriter(table_name)
    count = writer.row_count()
    print(f"\nTable: streamlake.{table_name}  ({count:,} total rows)\n")
    df = writer.scan(limit=limit)
    if df.empty:
        print("  (no rows yet)")
    else:
        print(df.to_string(index=False))
    print()


def main():
    parser = argparse.ArgumentParser(prog="streamlake", description="StreamLake runner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("api", help="Start the control plane API on :8000")

    prod_p = sub.add_parser("producer", help="Start the fake order event producer")
    prod_p.add_argument("--servers", default="localhost:9092")
    prod_p.add_argument("--topic",   default="orders")
    prod_p.add_argument("--rate",    type=float, default=2.0, help="events/sec")
    prod_p.add_argument("--count",   type=int,   default=0,   help="0 = infinite")

    q_p = sub.add_parser("query", help="Scan latest rows from an Iceberg table")
    q_p.add_argument("table", help="Iceberg table name (e.g. 'orders')")
    q_p.add_argument("--limit", type=int, default=20)

    args, extra = parser.parse_known_args()

    if args.cmd == "api":
        run_api()
    elif args.cmd == "producer":
        argv = []
        for k, v in vars(args).items():
            if k == "cmd":
                continue
            argv += [f"--{k}", str(v)]
        run_producer(argv)
    elif args.cmd == "query":
        run_query(args.table, args.limit)


if __name__ == "__main__":
    main()
