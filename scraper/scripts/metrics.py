#!/usr/bin/env python3
"""
Print simple operational metrics for the GATC TEXT scraper.

Usage examples:
  python scripts/metrics.py --db-host 127.0.0.1
  python scripts/metrics.py --sql-conn your-project:your-region:your-instance
"""
import argparse

import psycopg2

DEFAULT_SQL_CONN = "your-project:your-region:your-instance"


def sql_connect(args):
    if args.db_host:
        return psycopg2.connect(dbname="adsdb", user="postgres", host=args.db_host, port=args.db_port or 5432)
    return psycopg2.connect(dbname="adsdb", user="postgres", host=f"/cloudsql/{args.sql_conn}")


def run_query(con, sql):
    with con.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return cols, rows


def print_table(title, cols, rows):
    print(f"\n== {title} ==")
    if not rows:
        print("(no rows)")
        return
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    fmt = "  " + " | ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*cols))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*[str(x) for x in r]))


def main():
    ap = argparse.ArgumentParser(description="Print scraper metrics from Cloud SQL")
    ap.add_argument("--sql-conn", default=DEFAULT_SQL_CONN, help="Cloud SQL connection name if using sockets")
    ap.add_argument("--db-host", help="Host for TCP connection (e.g., 127.0.0.1 when using cloud-sql-proxy)")
    ap.add_argument("--db-port", type=int)
    args = ap.parse_args()

    con = sql_connect(args)

    sections = [
        ("Status counts", "SELECT * FROM v_ads_status_counts ORDER BY count DESC"),
        ("Renderer counts", "SELECT * FROM v_ads_renderer_counts ORDER BY count DESC"),
        ("Top errors", "SELECT * FROM v_ads_error_counts"),
        (
            "Throughput (last 14 days)",
            "SELECT * FROM v_ads_daily_throughput WHERE day >= CURRENT_DATE - INTERVAL '14 days' ORDER BY day DESC",
        ),
        ("Renderer by year", "SELECT * FROM v_renderer_by_year"),
        ("Variation unavailable", "SELECT * FROM v_variation_unavailable"),
    ]

    for title, sql in sections:
        try:
            cols, rows = run_query(con, sql)
            print_table(title, cols, rows)
        except Exception as e:
            print(f"\n== {title} ==\nERROR: {e}")

    con.close()


if __name__ == "__main__":
    main()
