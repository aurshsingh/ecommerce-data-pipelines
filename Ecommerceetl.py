#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
 E-COMMERCE ETL  -  PRODUCTION VERSION           Python + pandas + MySQL
══════════════════════════════════════════════════════════════════════════════

 Replaces day2cleaning.py + day3mysqlconnect.py in ONE re-runnable script.

   olist_orders_dataset.csv      ┐
   olist_customers_dataset.csv   ├─> extract -> transform -> validate -> load
   olist_order_payments_dataset.csv ┘                                     │
                                                                          ▼
                                         fact_orders (one row per order, upserted)
                                         etl_rejected_orders (quarantined rows)
                                         etl_run_audit (one row per run)

 USAGE
 -----
   pip install pandas mysql-connector-python

   export MYSQL_PASSWORD='your-password'        # or you will be prompted
   python ecommerce_etl.py --dry-run            # extract+transform+validate only
   python ecommerce_etl.py                      # full run, loads MySQL
   python ecommerce_etl.py --export-csv orders_cleaned.csv

 ENVIRONMENT VARIABLES (all optional except the password)
 --------------------------------------------------------
   MYSQL_HOST (localhost)   MYSQL_PORT (3306)   MYSQL_USER (root)
   MYSQL_PASSWORD           MYSQL_DATABASE (ecommerce)
   ETL_DATA_DIR (.)         ETL_MIN_RETENTION (0.95)
   ETL_MAX_ORPHAN_RATE (0.01)   ETL_MAX_VOLUME_DROP (0.50)

 EXIT CODES
 ----------
   0 success | 1 expected failure (bad source / failed validation / load error)
   2 unexpected error

 WHAT THIS FIXES vs the day2/day3 scripts
 ----------------------------------------
 1. PAYMENT FAN-OUT. Joining raw payments onto orders duplicated every order that
    was paid in several parts, so COUNT(*) counted payments, not orders. Payments
    are now aggregated to ONE row per order BEFORE the join.
 2. SAFE RE-RUNS. The old script did DROP TABLE and reloaded everything. Now:
    staging (temp table) -> hash diff -> UPSERT, all in ONE transaction. A failed
    run rolls back and leaves yesterday's data untouched; a re-run never
    duplicates rows.
 3. DATA QUALITY GATE. Bad rows are quarantined with a reason (not silently
    dropped) and validation checks stop the load if the data looks wrong.
 4. SCHEMA. Real DATETIME/DATE/DECIMAL types, primary key, indexes (FLOAT for
    money was replaced by DECIMAL).
 5. OPERATIONS. Logging to console + rotating file, no password in code,
    connection retries, an advisory lock against two runs at once, an audit row
    for every run (success AND failure), and meaningful exit codes.

 KNOWN LIMITS
 ------------
   * Orders deleted at the source are not deleted from fact_orders.
   * The whole load is one transaction: fine for ~100k rows; for tens of millions
     switch to batched commits.
   * VALUES() in ON DUPLICATE KEY UPDATE is deprecated in MySQL 8.0.20+ but works.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import logging
import logging.handlers
import os
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import mysql.connector
from mysql.connector import Error as MySQLError

log = logging.getLogger("ecommerce_etl")


# ══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ══════════════════════════════════════════════════════════════════════════════

class ETLError(Exception):
    """Base class for expected, explainable failures (exit code 1)."""


class SourceError(ETLError):
    """Source file missing, empty, or its columns changed."""


class ValidationError(ETLError):
    """Data-quality gate failed."""


class LoadError(ETLError):
    """Database problem or reconciliation mismatch."""


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS  -  schema is defined ONCE and reused for DDL, staging, upsert, checks
# ══════════════════════════════════════════════════════════════════════════════

FACT_TABLE = "fact_orders"
STAGING_TABLE = "stg_orders"          # TEMPORARY table, private to this connection
AUDIT_TABLE = "etl_run_audit"
REJECT_TABLE = "etl_rejected_orders"
LOCK_NAME = "ecommerce_etl_load"

SOURCE_FILES = {
    "orders": (
        "olist_orders_dataset.csv",
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
         "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"],
    ),
    "customers": (
        "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        ["order_id", "payment_type", "payment_installments", "payment_value"],
    ),
}

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"   # strict: any other format becomes NaT and is caught

ALLOWED_STATUSES = {
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
}

ORDER_COLUMNS = [
    ("order_id", "VARCHAR(32) NOT NULL"),
    ("customer_id", "VARCHAR(32) NOT NULL"),
    ("customer_unique_id", "VARCHAR(32) NULL"),
    ("customer_city", "VARCHAR(100) NULL"),
    ("customer_state", "VARCHAR(10) NULL"),
    ("order_status", "VARCHAR(20) NOT NULL"),
    ("order_purchase_timestamp", "DATETIME NOT NULL"),
    ("order_approved_at", "DATETIME NULL"),
    ("order_delivered_carrier_date", "DATETIME NULL"),
    ("order_delivered_customer_date", "DATETIME NULL"),
    ("order_estimated_delivery_date", "DATETIME NULL"),
    ("purchase_date", "DATE NOT NULL"),
    ("year", "SMALLINT NOT NULL"),
    ("month", "TINYINT NOT NULL"),
    ("day", "TINYINT NOT NULL"),
    ("quarter", "TINYINT NOT NULL"),
    ("delivery_days", "DECIMAL(8,2) NULL"),
    ("approval_hours", "DECIMAL(10,2) NULL"),
    ("is_late", "TINYINT(1) NULL"),
    ("payment_total", "DECIMAL(12,2) NULL"),
    ("payment_count", "SMALLINT NOT NULL"),
    ("payment_installments_max", "SMALLINT NULL"),
    ("payment_type_main", "VARCHAR(30) NULL"),
    ("row_hash", "CHAR(32) NOT NULL"),
]
COLUMN_NAMES = [name for name, _ in ORDER_COLUMNS]
HASH_COLUMNS = [c for c in COLUMN_NAMES if c != "row_hash"]
NOT_NULL_COLUMNS = [name for name, ddl in ORDER_COLUMNS if "NOT NULL" in ddl]
MAX_LENGTHS = {
    name: int(ddl.split("(")[1].split(")")[0])
    for name, ddl in ORDER_COLUMNS if ddl.startswith(("VARCHAR", "CHAR"))
}


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG / RUN CONTEXT
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class Config:
    data_dir: Path
    host: str
    port: int
    user: str
    password: str
    database: str
    batch_size: int
    min_retention: float
    max_orphan_rate: float
    max_volume_drop: float
    export_csv: Path | None
    dry_run: bool


@dataclass
class RunInfo:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: _utcnow())
    stage: str = "startup"
    orders_md5: str | None = None
    rows_extracted: int | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)   # MySQL DATETIME is naive


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production-style e-commerce ETL (CSV -> MySQL).")
    parser.add_argument("--data-dir", default=os.environ.get("ETL_DATA_DIR", "."),
                        help="folder containing the Olist CSV files")
    parser.add_argument("--dry-run", action="store_true",
                        help="run extract/transform/validate only; do not touch MySQL")
    parser.add_argument("--export-csv", default=None,
                        help="also write the cleaned dataset to this CSV path")
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="rows per INSERT batch (default 5000)")
    parser.add_argument("--log-dir", default="logs", help="folder for the log file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> Config:
    password = os.environ.get("MYSQL_PASSWORD", "")
    if not password and not args.dry_run:
        if sys.stdin.isatty():
            password = getpass.getpass("MySQL password: ")
        else:
            raise ETLError("MYSQL_PASSWORD is not set and no terminal to prompt on")
    return Config(
        data_dir=Path(args.data_dir),
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=password,
        database=os.environ.get("MYSQL_DATABASE", "ecommerce"),
        batch_size=args.batch_size,
        min_retention=float(os.environ.get("ETL_MIN_RETENTION", "0.95")),
        max_orphan_rate=float(os.environ.get("ETL_MAX_ORPHAN_RATE", "0.01")),
        max_volume_drop=float(os.environ.get("ETL_MAX_VOLUME_DROP", "0.50")),
        export_csv=Path(args.export_csv) if args.export_csv else None,
        dry_run=args.dry_run,
    )


def setup_logging(level: str, log_dir: str) -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logfile = logging.handlers.RotatingFileHandler(
        Path(log_dir) / "ecommerce_etl.log", maxBytes=5_000_000, backupCount=5,
        encoding="utf-8")
    logfile.setFormatter(fmt)
    log.setLevel(level)
    log.handlers[:] = [console, logfile]


# ══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

def _md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract(cfg: Config, run: RunInfo) -> dict[str, pd.DataFrame]:
    """Read the source CSVs as text and enforce the column contract."""
    frames: dict[str, pd.DataFrame] = {}
    for name, (filename, required) in SOURCE_FILES.items():
        path = cfg.data_dir / filename
        if not path.exists() or path.stat().st_size == 0:
            raise SourceError(f"Source file missing or empty: {path}")

        df = pd.read_csv(path, dtype=str)
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise SourceError(f"Schema drift in {filename} - missing columns: {missing}")

        frames[name] = df
        log.info("Extracted %-10s %8d rows  (%s)", name, len(df), filename)
        if name == "orders":
            run.orders_md5 = _md5_file(path)
            run.rows_extracted = len(df)
    return frames


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM  (pure functions: no database, no files  ->  easy to unit-test)
# ══════════════════════════════════════════════════════════════════════════════

def build_payment_summary(payments_raw: pd.DataFrame):
    """
    Collapse payments to ONE row per order  (this removes the join fan-out).

    Returns (summary_df, invalid_payment_rows).
    payment_type_main = the payment type that carried the most money.
    """
    pay = payments_raw[["order_id", "payment_type",
                        "payment_installments", "payment_value"]].copy()
    pay["order_id"] = pay["order_id"].str.strip()
    pay["payment_type"] = pay["payment_type"].str.strip().str.lower()
    pay["payment_value"] = pd.to_numeric(pay["payment_value"], errors="coerce")
    pay["payment_installments"] = pd.to_numeric(pay["payment_installments"], errors="coerce")

    invalid = pay["order_id"].isna() | pay["payment_value"].isna() | (pay["payment_value"] < 0)
    invalid_rows = int(invalid.sum())
    pay = pay[~invalid]

    totals = pay.groupby("order_id").agg(
        payment_total=("payment_value", "sum"),
        payment_count=("payment_value", "size"),
        payment_installments_max=("payment_installments", "max"),
    ).reset_index()

    by_type = pay.groupby(["order_id", "payment_type"], as_index=False)["payment_value"].sum()
    main_type = (
        by_type.sort_values(["order_id", "payment_value", "payment_type"],
                            ascending=[True, False, True])
        .drop_duplicates("order_id")[["order_id", "payment_type"]]
        .rename(columns={"payment_type": "payment_type_main"})
    )

    summary = totals.merge(main_type, on="order_id", how="left")
    summary["payment_total"] = summary["payment_total"].round(2)
    summary["payment_installments_max"] = summary["payment_installments_max"].astype("Int16")
    return summary, invalid_rows


def build_clean_orders(orders_raw: pd.DataFrame,
                       customers_raw: pd.DataFrame,
                       payments_raw: pd.DataFrame):
    """
    Clean and enrich orders. Returns (clean_df, rejected_df, metrics).
    Every input row lands in exactly one of clean_df / rejected_df.
    A rejected row gets the FIRST matching reason (rules run in this order).
    """
    raw = orders_raw.copy()
    orders = raw.copy()

    # standardise text
    for col in ("order_id", "customer_id", "order_status"):
        orders[col] = orders[col].str.strip()
    orders["order_status"] = orders["order_status"].str.lower()

    # parse timestamps strictly and count failures
    unparseable = {}
    for col in TIMESTAMP_COLUMNS:
        parsed = pd.to_datetime(orders[col], format=TS_FORMAT, errors="coerce")
        unparseable[col] = int((orders[col].notna() & parsed.isna()).sum())
        orders[col] = parsed

    # reject rules
    reason = pd.Series(pd.NA, index=orders.index, dtype="object")

    def flag(mask: pd.Series, label: str) -> None:
        reason.loc[mask & reason.isna()] = label

    flag(orders["order_id"].isna() | (orders["order_id"] == ""), "missing_order_id")
    flag(orders["customer_id"].isna() | (orders["customer_id"] == ""), "missing_customer_id")
    flag(orders["order_purchase_timestamp"].isna(), "invalid_purchase_timestamp")
    flag(~orders["order_status"].isin(ALLOWED_STATUSES), "invalid_status")
    flag(orders["order_approved_at"].isna(), "missing_approval")
    flag(orders["order_approved_at"] < orders["order_purchase_timestamp"],
         "approval_before_purchase")
    flag(orders["order_delivered_customer_date"] < orders["order_purchase_timestamp"],
         "delivery_before_purchase")

    survivors = orders[reason.isna()]
    dup_index = survivors.index[survivors.duplicated(subset=["order_id"], keep="first")]
    flag(pd.Series(orders.index.isin(dup_index), index=orders.index), "duplicate_order_id")

    is_rejected = reason.notna()
    clean = orders[~is_rejected].copy()
    rejected = raw[is_rejected].copy()
    rejected["reject_reason"] = reason[is_rejected]

    # customers (made unique on customer_id so the join cannot multiply rows)
    customers = customers_raw[SOURCE_FILES["customers"][1]].copy()
    customers["customer_id"] = customers["customer_id"].str.strip()
    duplicate_customers = int(customers.duplicated(subset=["customer_id"]).sum())
    customers = customers.drop_duplicates(subset=["customer_id"])
    clean = clean.merge(customers, on="customer_id", how="left", validate="many_to_one")
    orphan_orders = int(clean["customer_unique_id"].isna().sum())

    # payments (already one row per order)
    payments, invalid_payments = build_payment_summary(payments_raw)
    clean = clean.merge(payments, on="order_id", how="left", validate="one_to_one")
    clean["payment_count"] = clean["payment_count"].fillna(0).astype(int)
    orders_without_payment = int((clean["payment_count"] == 0).sum())

    # derived columns
    purchase = clean["order_purchase_timestamp"]
    delivered = clean["order_delivered_customer_date"]
    estimated = clean["order_estimated_delivery_date"]
    clean["purchase_date"] = purchase.dt.date
    clean["year"] = purchase.dt.year
    clean["month"] = purchase.dt.month
    clean["day"] = purchase.dt.day
    clean["quarter"] = purchase.dt.quarter
    clean["delivery_days"] = ((delivered - purchase).dt.total_seconds() / 86400).round(2)
    clean["approval_hours"] = (
        (clean["order_approved_at"] - purchase).dt.total_seconds() / 3600).round(2)
    clean["is_late"] = (delivered > estimated).astype("Int8").where(
        delivered.notna() & estimated.notna())

    # row hash: lets the load detect NEW / CHANGED rows. NULLs get an explicit
    # marker first so the hash is identical on every pandas version.
    clean = clean[HASH_COLUMNS].reset_index(drop=True)
    as_text = clean.astype(object).where(clean.notna(), "\x00NULL").astype(str)
    clean["row_hash"] = as_text.agg("|".join, axis=1).map(
        lambda s: hashlib.md5(s.encode("utf-8")).hexdigest())
    clean = clean[COLUMN_NAMES]

    metrics = {
        "rows_raw": int(len(raw)),
        "rows_clean": int(len(clean)),
        "rows_rejected": int(len(rejected)),
        "reject_reasons": {k: int(v) for k, v in reason.dropna().value_counts().items()},
        "duplicate_customers": duplicate_customers,
        "orphan_orders": orphan_orders,
        "orders_without_payment": orders_without_payment,
        "invalid_payment_rows": invalid_payments,
        "unparseable_timestamps": {k: v for k, v in unparseable.items() if v},
        "revenue_total": round(float(clean["payment_total"].sum()), 2),
    }
    return clean, rejected, metrics


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATE
# ══════════════════════════════════════════════════════════════════════════════

def run_quality_checks(df: pd.DataFrame, metrics: dict, previous_clean_rows,
                       cfg: Config):
    """Returns (failures, warnings, passed). Collects EVERY failure at once."""
    failures: list[str] = []
    warnings: list[str] = []
    passed = 0

    def check(ok: bool, message: str) -> None:
        nonlocal passed
        if ok:
            passed += 1
        else:
            failures.append(message)

    check(len(df) > 0, "dataset is empty")
    if len(df) == 0:
        return failures, warnings, passed

    check(list(df.columns) == COLUMN_NAMES, "columns do not match the target schema")
    check(len(df) == metrics["rows_clean"], "row count differs from transform metrics")

    check(bool(df["order_id"].notna().all()), "null order_id values found")
    dups = int(df["order_id"].duplicated().sum())
    check(dups == 0, f"{dups} duplicate order_id values (payment fan-out?)")

    for col in NOT_NULL_COLUMNS:
        nulls = int(df[col].isna().sum())
        check(nulls == 0, f"{nulls} nulls in NOT NULL column '{col}'")

    for col, limit in MAX_LENGTHS.items():
        longest = int(df[col].dropna().astype(str).str.len().max() or 0)
        check(longest <= limit, f"'{col}' has a value of {longest} chars (column allows {limit})")

    latest = pd.to_datetime(df["order_purchase_timestamp"]).max()
    check(latest <= pd.Timestamp.now() + pd.Timedelta(days=1),
          f"future purchase timestamp: {latest}")

    bad_status = sorted(set(df["order_status"]) - ALLOWED_STATUSES)
    check(not bad_status, f"unexpected order_status values: {bad_status}")

    check(bool((df["delivery_days"].dropna() >= 0).all()), "negative delivery_days found")
    check(bool((df["payment_total"].dropna() >= 0).all()), "negative payment_total found")

    retention = metrics["rows_clean"] / metrics["rows_raw"] if metrics["rows_raw"] else 0
    check(retention >= cfg.min_retention,
          f"retention {retention:.2%} below minimum {cfg.min_retention:.0%}")

    orphan_rate = metrics["orphan_orders"] / max(metrics["rows_clean"], 1)
    check(orphan_rate <= cfg.max_orphan_rate,
          f"{orphan_rate:.2%} of orders have no customer (max {cfg.max_orphan_rate:.0%})")

    if previous_clean_rows:
        floor = previous_clean_rows * (1 - cfg.max_volume_drop)
        check(metrics["rows_clean"] >= floor,
              f"volume dropped: {metrics['rows_clean']} rows vs {previous_clean_rows} last success")

    # non-fatal observations
    if metrics["orders_without_payment"]:
        warnings.append(f"{metrics['orders_without_payment']} orders have no payment record")
    if metrics["invalid_payment_rows"]:
        warnings.append(f"{metrics['invalid_payment_rows']} invalid payment rows were ignored")
    if metrics["unparseable_timestamps"]:
        warnings.append(f"unparseable timestamps: {metrics['unparseable_timestamps']}")

    return failures, warnings, passed


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def _q(identifier: str) -> str:
    return f"`{identifier}`"


def _col_ddl(extra: list[str]) -> str:
    return ",\n  ".join([f"{_q(n)} {t}" for n, t in ORDER_COLUMNS] + extra)


def fact_ddl() -> str:
    body = _col_ddl([
        "`etl_run_id` VARCHAR(36) NOT NULL",
        "`etl_loaded_at` DATETIME NOT NULL",
        "PRIMARY KEY (`order_id`)",
        "KEY `idx_purchase_date` (`purchase_date`)",
        "KEY `idx_year_month` (`year`, `month`)",
        "KEY `idx_state` (`customer_state`)",
        "KEY `idx_status` (`order_status`)",
        "KEY `idx_customer` (`customer_id`)",
    ])
    return (f"CREATE TABLE IF NOT EXISTS {_q(FACT_TABLE)} (\n  {body}\n) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")


def staging_ddl() -> str:
    body = _col_ddl(["PRIMARY KEY (`order_id`)"])
    return f"CREATE TEMPORARY TABLE {_q(STAGING_TABLE)} (\n  {body}\n) ENGINE=InnoDB"


AUDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS {_q(AUDIT_TABLE)} (
  `run_id` VARCHAR(36) NOT NULL,
  `started_at` DATETIME NOT NULL,
  `finished_at` DATETIME NULL,
  `status` VARCHAR(10) NOT NULL,
  `failed_stage` VARCHAR(30) NULL,
  `message` TEXT NULL,
  `source_orders_md5` CHAR(32) NULL,
  `rows_extracted` INT NULL,
  `rows_clean` INT NULL,
  `rows_rejected` INT NULL,
  `rows_inserted` INT NULL,
  `rows_updated` INT NULL,
  `rows_unchanged` INT NULL,
  `target_total_rows` INT NULL,
  `duration_seconds` DECIMAL(10,2) NULL,
  PRIMARY KEY (`run_id`),
  KEY `idx_status_finished` (`status`, `finished_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

REJECT_DDL = f"""
CREATE TABLE IF NOT EXISTS {_q(REJECT_TABLE)} (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `etl_run_id` VARCHAR(36) NOT NULL,
  `order_id` VARCHAR(64) NULL,
  `customer_id` VARCHAR(64) NULL,
  `order_status` VARCHAR(32) NULL,
  `order_purchase_timestamp` VARCHAR(32) NULL,
  `reject_reason` VARCHAR(50) NOT NULL,
  `rejected_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_run` (`etl_run_id`),
  KEY `idx_reason` (`reject_reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _py(value):
    """pandas/numpy scalar -> plain Python value the MySQL driver accepts."""
    if value is None or value is pd.NaT:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def iter_row_batches(df: pd.DataFrame, batch_size: int):
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start:start + batch_size]
        yield [tuple(_py(v) for v in row)
               for row in chunk.itertuples(index=False, name=None)]


def _text(value):
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def connect_with_retry(cfg: Config, attempts: int = 3):
    for attempt in range(1, attempts + 1):
        try:
            conn = mysql.connector.connect(
                host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
                database=cfg.database, charset="utf8mb4", connection_timeout=10,
                autocommit=False)
            log.info("Connected to MySQL %s:%s/%s", cfg.host, cfg.port, cfg.database)
            return conn
        except MySQLError as exc:
            log.warning("MySQL connection attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt == attempts:
                raise LoadError(f"Could not connect to MySQL: {exc}") from exc
            time.sleep(2 ** attempt)


@contextmanager
def advisory_lock(conn, name: str):
    """Stops two ETL runs from loading at the same time."""
    cur = conn.cursor()
    cur.execute("SELECT GET_LOCK(%s, 0)", (name,))
    got = cur.fetchone()[0]
    cur.close()
    if got != 1:
        raise LoadError("Another ETL run holds the lock - refusing to run concurrently")
    try:
        yield
    finally:
        try:
            cur = conn.cursor()
            cur.execute("SELECT RELEASE_LOCK(%s)", (name,))
            cur.fetchone()
            cur.close()
        except MySQLError:
            log.warning("Could not release advisory lock (released on disconnect)")


def ensure_tables(conn) -> None:
    cur = conn.cursor()
    for ddl in (fact_ddl(), AUDIT_DDL, REJECT_DDL):
        cur.execute(ddl)
    conn.commit()

    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (FACT_TABLE,))
    actual = [_text(r[0]) for r in cur.fetchall()]
    cur.close()
    expected = COLUMN_NAMES + ["etl_run_id", "etl_loaded_at"]
    if actual != expected:
        raise LoadError(
            f"Table {FACT_TABLE} exists with a different structure than this script "
            f"expects. Rename or drop it and re-run.\n  found:    {actual}\n  expected: {expected}")


def previous_clean_rows(conn):
    cur = conn.cursor()
    cur.execute(
        f"SELECT `rows_clean` FROM {_q(AUDIT_TABLE)} "
        "WHERE `status` = 'SUCCESS' ORDER BY `finished_at` DESC LIMIT 1")
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] is not None else None


def upsert_sql() -> str:
    cols = ", ".join(_q(c) for c in COLUMN_NAMES)
    updates = ", ".join(f"{_q(c)} = VALUES({_q(c)})" for c in COLUMN_NAMES if c != "order_id")
    updates += ", `etl_run_id` = VALUES(`etl_run_id`), `etl_loaded_at` = VALUES(`etl_loaded_at`)"
    return (f"INSERT INTO {_q(FACT_TABLE)} ({cols}, `etl_run_id`, `etl_loaded_at`) "
            f"SELECT {cols}, %s, %s FROM {_q(STAGING_TABLE)} "
            f"ON DUPLICATE KEY UPDATE {updates}")


def _scalar(cur, sql: str, params=()):
    cur.execute(sql, params)
    value = cur.fetchone()[0]
    return int(value) if value is not None else 0


def _write_audit(cur, run: RunInfo, status: str, **fields) -> None:
    row = {
        "run_id": run.run_id, "started_at": run.started_at, "finished_at": _utcnow(),
        "status": status, "source_orders_md5": run.orders_md5,
        "rows_extracted": run.rows_extracted,
        "duration_seconds": round((_utcnow() - run.started_at).total_seconds(), 2),
        **fields,
    }
    cols = list(row)
    updates = ", ".join(f"{_q(c)} = VALUES({_q(c)})" for c in cols if c != "run_id")
    cur.execute(
        f"INSERT INTO {_q(AUDIT_TABLE)} ({', '.join(_q(c) for c in cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cols))}) ON DUPLICATE KEY UPDATE {updates}",
        tuple(row[c] for c in cols))


def load_to_mysql(conn, clean: pd.DataFrame, rejected: pd.DataFrame,
                  metrics: dict, run: RunInfo, cfg: Config) -> dict:
    """
    Stage -> diff -> upsert -> reconcile -> audit, as ONE transaction.
    Any error rolls everything back; fact_orders is never left half-loaded.
    """
    cur = conn.cursor()
    loaded_at = _utcnow()
    try:
        cur.execute(staging_ddl())      # TEMPORARY: no implicit commit, private to us

        for batch in iter_row_batches(clean, cfg.batch_size):
            placeholders = ", ".join(["%s"] * len(COLUMN_NAMES))
            cur.executemany(
                f"INSERT INTO {_q(STAGING_TABLE)} "
                f"({', '.join(_q(c) for c in COLUMN_NAMES)}) VALUES ({placeholders})",
                batch)
        staged = _scalar(cur, f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)}")
        if staged != len(clean):
            raise LoadError(f"Staging holds {staged} rows but {len(clean)} were sent")

        # keep only NEW or CHANGED rows (same order_id + same hash = unchanged)
        cur.execute(
            f"DELETE s FROM {_q(STAGING_TABLE)} s "
            f"JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "AND f.`row_hash` = s.`row_hash`")
        delta = _scalar(cur, f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)}")
        new_rows = _scalar(
            cur,
            f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)} s "
            f"LEFT JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "WHERE f.`order_id` IS NULL")
        log.info("Staged %d rows -> delta %d (new %d, changed %d, unchanged %d)",
                 staged, delta, new_rows, delta - new_rows, staged - delta)

        if delta:
            cur.execute(upsert_sql(), (run.run_id, loaded_at))

        # reconcile: every delta row must now exist in the target with the same hash
        verified = _scalar(
            cur,
            f"SELECT COUNT(*) FROM {_q(STAGING_TABLE)} s "
            f"JOIN {_q(FACT_TABLE)} f ON f.`order_id` = s.`order_id` "
            "AND f.`row_hash` = s.`row_hash`")
        if verified != delta:
            raise LoadError(f"Reconciliation failed: {verified} of {delta} rows verified")
        total = _scalar(cur, f"SELECT COUNT(*) FROM {_q(FACT_TABLE)}")

        # quarantine rows from THIS run (run_id makes this idempotent)
        if len(rejected):
            cur.execute(f"DELETE FROM {_q(REJECT_TABLE)} WHERE `etl_run_id` = %s", (run.run_id,))
            rej = rejected[["order_id", "customer_id", "order_status",
                            "order_purchase_timestamp", "reject_reason"]]

            def clip(value, size):
                value = _py(value)
                return value[:size] if isinstance(value, str) else value

            rej_rows = [
                (run.run_id, clip(a, 64), clip(b, 64), clip(c, 32), clip(d, 32), e, loaded_at)
                for a, b, c, d, e in rej.itertuples(index=False, name=None)
            ]
            cur.executemany(
                f"INSERT INTO {_q(REJECT_TABLE)} (`etl_run_id`, `order_id`, `customer_id`, "
                "`order_status`, `order_purchase_timestamp`, `reject_reason`, `rejected_at`) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)", rej_rows)

        stats = {"staged": staged, "delta": delta, "inserted": new_rows,
                 "updated": delta - new_rows, "unchanged": staged - delta,
                 "target_total": total}
        _write_audit(cur, run, "SUCCESS",
                     rows_clean=metrics["rows_clean"], rows_rejected=metrics["rows_rejected"],
                     rows_inserted=new_rows, rows_updated=delta - new_rows,
                     rows_unchanged=staged - delta, target_total_rows=total)
        conn.commit()
        return stats
    except Exception as exc:
        conn.rollback()
        log.error("Load failed - transaction rolled back, %s is unchanged", FACT_TABLE)
        if isinstance(exc, MySQLError):
            raise LoadError(f"Database error during load: {exc}") from exc
        raise
    finally:
        try:
            cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {_q(STAGING_TABLE)}")
        except MySQLError:
            pass
        cur.close()


def record_failure(conn, run: RunInfo, exc: Exception) -> None:
    """Best-effort FAILED audit row. Never raises."""
    try:
        conn.rollback()
        cur = conn.cursor()
        _write_audit(cur, run, "FAILED", failed_stage=run.stage, message=str(exc)[:4000])
        conn.commit()
        cur.close()
    except Exception:  # noqa: BLE001
        log.exception("Could not write the FAILED audit row")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(cfg: Config, run: RunInfo, state: dict) -> int:
    clock = time.perf_counter()

    run.stage = "extract"
    frames = extract(cfg, run)

    run.stage = "transform"
    clean, rejected, metrics = build_clean_orders(
        frames["orders"], frames["customers"], frames["payments"])
    log.info("Transform: %d clean, %d rejected %s", metrics["rows_clean"],
             metrics["rows_rejected"], metrics["reject_reasons"] or "")

    conn = None
    if not cfg.dry_run:
        run.stage = "connect"
        conn = connect_with_retry(cfg)
        state["conn"] = conn

    try:
        previous = None
        if conn is not None:
            run.stage = "prepare"
            with advisory_lock(conn, LOCK_NAME):
                ensure_tables(conn)
                previous = previous_clean_rows(conn)

                run.stage = "validate"
                failures, warnings, passed = run_quality_checks(clean, metrics, previous, cfg)
                _report_validation(failures, warnings, passed)

                run.stage = "load"
                stats = load_to_mysql(conn, clean, rejected, metrics, run, cfg)
        else:
            run.stage = "validate"
            failures, warnings, passed = run_quality_checks(clean, metrics, None, cfg)
            _report_validation(failures, warnings, passed)
            stats = None

        if cfg.export_csv:
            run.stage = "export"
            clean.to_csv(cfg.export_csv, index=False)
            log.info("Exported cleaned data to %s", cfg.export_csv)
    except Exception:
        raise            # main() records the FAILED audit row, then closes the connection

    if conn is not None and conn.is_connected():
        conn.close()
        log.info("Connection closed")

    _print_summary(metrics, stats, time.perf_counter() - clock, cfg.dry_run)
    return 0


def _report_validation(failures, warnings, passed) -> None:
    for item in warnings:
        log.warning("WARNING: %s", item)
    if failures:
        for item in failures:
            log.error("VALIDATION FAILED: %s", item)
        raise ValidationError(f"{len(failures)} data-quality check(s) failed: "
                              + "; ".join(failures))
    log.info("Validation passed (%d checks)", passed)


def _print_summary(metrics: dict, stats: dict | None, seconds: float, dry_run: bool) -> None:
    line = "=" * 58
    log.info(line)
    log.info("ETL %s", "DRY RUN COMPLETE (nothing written)" if dry_run else "COMPLETED SUCCESSFULLY")
    log.info(line)
    log.info("Source orders      : %d", metrics["rows_raw"])
    log.info("Clean orders       : %d", metrics["rows_clean"])
    log.info("Rejected           : %d  %s", metrics["rows_rejected"], metrics["reject_reasons"])
    log.info("Total revenue      : %.2f", metrics["revenue_total"])
    if stats:
        log.info("Inserted           : %d", stats["inserted"])
        log.info("Updated            : %d", stats["updated"])
        log.info("Unchanged (skipped): %d", stats["unchanged"])
        log.info("Rows in %-13s: %d", FACT_TABLE, stats["target_total"])
    log.info("Duration           : %.1fs", seconds)
    log.info(line)


def _fail(state: dict, run: RunInfo, exc: Exception) -> None:
    """Write the FAILED audit row (if we have a connection), then close it."""
    conn = state.get("conn")
    if conn is not None and conn.is_connected():
        record_failure(conn, run, exc)
        conn.close()
        log.info("Connection closed")


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_dir)
    run = RunInfo()
    state: dict = {}
    log.info("Run %s started", run.run_id)

    try:
        cfg = build_config(args)
        return run_pipeline(cfg, run, state)
    except ETLError as exc:
        log.error("Run failed during '%s': %s", run.stage, exc)
        _fail(state, run, exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        log.exception("Unexpected error during '%s'", run.stage)
        _fail(state, run, exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
