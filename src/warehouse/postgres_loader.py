"""Silver layer loader for staging raw S3 JSON payloads into PostgreSQL."""

import json
import logging
import sys
from typing import Any

import boto3
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from src.config import S3Config, WarehouseConfig, load_s3_config, load_warehouse_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline.warehouse.postgres")


DDL_STAGING_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_fx_rates (
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    exchange_rate NUMERIC(12, 6) NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (rate_date, base_currency, target_currency)
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    category VARCHAR(100) NOT NULL,
    base_price_eur NUMERIC(10, 2) NOT NULL,
    rating_score NUMERIC(3, 2),
    rating_count INTEGER,
    ingested_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    order_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, product_id)
);
"""


class PostgresStagingLoader:
    """Reads latest partitioned files from S3 Bronze layer and upserts into PostgreSQL."""

    def __init__(self, s3_cfg: S3Config, wh_cfg: WarehouseConfig) -> None:
        self.s3_cfg = s3_cfg
        self.wh_cfg = wh_cfg
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.s3_cfg.access_key,
            aws_secret_access_key=self.s3_cfg.secret_key,
            region_name=self.s3_cfg.region,
        )

    def _get_db_connection(self) -> PgConnection:
        return psycopg2.connect(
            host=self.wh_cfg.host,
            port=self.wh_cfg.port,
            dbname=self.wh_cfg.database,
            user=self.wh_cfg.user,
            password=self.wh_cfg.password,
            sslmode="require",
        )

    def fetch_latest_s3_envelope(self, domain: str) -> dict[str, Any]:
        prefix = f"raw/{domain}/"
        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_cfg.bucket_name,
            Prefix=prefix,
        )
        contents = response.get("Contents", [])
        if not contents:
            raise FileNotFoundError(
                f"No S3 objects found in bucket '{self.s3_cfg.bucket_name}' with prefix '{prefix}'"
            )

        latest_object = max(contents, key=lambda obj: obj["LastModified"])
        latest_key = latest_object["Key"]
        logger.info("Reading latest S3 artifact for '%s': s3://%s/%s", domain, self.s3_cfg.bucket_name, latest_key)

        obj_response = self.s3_client.get_object(Bucket=self.s3_cfg.bucket_name, Key=latest_key)
        raw_body = obj_response["Body"].read().decode("utf-8")
        return json.loads(raw_body)

    def initialize_staging_schema(self, conn: PgConnection) -> None:
        with conn.cursor() as cur:
            cur.execute(DDL_STAGING_SCHEMA)
        conn.commit()
        logger.info("Verified staging schema and DDL definitions in PostgreSQL.")

    def upsert_fx_rates(self, conn: PgConnection, envelope: dict[str, Any]) -> int:
        ingested_at = envelope["metadata"]["ingested_at_utc"]
        payload = envelope["payload"]
        rate_date = payload["date"]
        base_currency = payload["base"]
        rates: dict[str, float] = payload.get("rates", {})

        rows = [
            (rate_date, base_currency, target_curr, float(rate_val), ingested_at)
            for target_curr, rate_val in rates.items()
        ]

        upsert_sql = """
            INSERT INTO staging.stg_fx_rates (
                rate_date, base_currency, target_currency, exchange_rate, ingested_at_utc
            )
            VALUES %s
            ON CONFLICT (rate_date, base_currency, target_currency) DO UPDATE SET
                exchange_rate = EXCLUDED.exchange_rate,
                ingested_at_utc = EXCLUDED.ingested_at_utc;
        """
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)
        conn.commit()
        logger.info("Upserted %d FX rate record(s) into staging.stg_fx_rates.", len(rows))
        return len(rows)

    def upsert_products(self, conn: PgConnection, envelope: dict[str, Any]) -> int:
        ingested_at = envelope["metadata"]["ingested_at_utc"]
        products: list[dict[str, Any]] = envelope["payload"]

        rows = []
        for item in products:
            rating = item.get("rating") or {}
            rows.append(
                (
                    int(item["id"]),
                    str(item["title"]).strip(),
                    str(item["category"]).strip(),
                    float(item["price"]),
                    float(rating.get("rate", 0.0)),
                    int(rating.get("count", 0)),
                    ingested_at,
                )
            )

        upsert_sql = """
            INSERT INTO staging.stg_products (
                product_id, title, category, base_price_eur, rating_score, rating_count, ingested_at_utc
            )
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                base_price_eur = EXCLUDED.base_price_eur,
                rating_score = EXCLUDED.rating_score,
                rating_count = EXCLUDED.rating_count,
                ingested_at_utc = EXCLUDED.ingested_at_utc;
        """
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)
        conn.commit()
        logger.info("Upserted %d product record(s) into staging.stg_products.", len(rows))
        return len(rows)

    def upsert_order_items(self, conn: PgConnection, envelope: dict[str, Any]) -> int:
        ingested_at = envelope["metadata"]["ingested_at_utc"]
        orders: list[dict[str, Any]] = envelope["payload"]

        rows = []
        for order in orders:
            order_id = int(order["id"])
            user_id = int(order["userId"])
            order_ts = order["date"]
            for line_item in order.get("products", []):
                rows.append(
                    (
                        order_id,
                        user_id,
                        int(line_item["productId"]),
                        int(line_item["quantity"]),
                        order_ts,
                        ingested_at,
                    )
                )

        upsert_sql = """
            INSERT INTO staging.stg_order_items (
                order_id, user_id, product_id, quantity, order_timestamp, ingested_at_utc
            )
            VALUES %s
            ON CONFLICT (order_id, product_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                quantity = EXCLUDED.quantity,
                order_timestamp = EXCLUDED.order_timestamp,
                ingested_at_utc = EXCLUDED.ingested_at_utc;
        """
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)
        conn.commit()
        logger.info("Upserted %d order line item(s) into staging.stg_order_items.", len(rows))
        return len(rows)

    def run_staging_load(self) -> None:
        fx_envelope = self.fetch_latest_s3_envelope("fx_rates")
        products_envelope = self.fetch_latest_s3_envelope("products")
        orders_envelope = self.fetch_latest_s3_envelope("orders")

        with self._get_db_connection() as conn:
            self.initialize_staging_schema(conn)
            self.upsert_fx_rates(conn, fx_envelope)
            self.upsert_products(conn, products_envelope)
            self.upsert_order_items(conn, orders_envelope)


def main() -> None:
    s3_cfg = load_s3_config()
    wh_cfg = load_warehouse_config()
    loader = PostgresStagingLoader(s3_cfg=s3_cfg, wh_cfg=wh_cfg)
    loader.run_staging_load()


if __name__ == "__main__":
    main()