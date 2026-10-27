"""Bronze layer ingestion client for extracting REST API payloads into AWS S3."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import APIConfig, S3Config, load_api_config, load_s3_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline.ingestion.s3")


class S3BronzeIngestor:
    """Extracts operational and financial datasets and lands them into S3 raw partitions."""

    def __init__(self, s3_cfg: S3Config, api_cfg: APIConfig) -> None:
        self.s3_cfg = s3_cfg
        self.api_cfg = api_cfg
        self.http_session = self._configure_http_session()
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.s3_cfg.access_key,
            aws_secret_access_key=self.s3_cfg.secret_key,
            region_name=self.s3_cfg.region,
        )

    def _configure_http_session(self) -> requests.Session:
        session = requests.Session()
        retry_policy = Retry(
            total=self.api_cfg.max_retries,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_json(self, endpoint_url: str, params: dict[str, Any] | None = None) -> Any:
        logger.info("Requesting data from endpoint: %s", endpoint_url)
        response = self.http_session.get(
            endpoint_url,
            params=params,
            timeout=self.api_cfg.request_timeout,
        )
        response.raise_for_status()
        return response.json()

    def upload_to_bronze(self, payload: Any, domain: str, source_url: str) -> str:
        now_utc = datetime.now(timezone.utc)
        partition_date = now_utc.strftime("%Y-%m-%d")
        file_timestamp = now_utc.strftime("%Y%m%d_%H%M%S")

        s3_key = f"raw/{domain}/ingestion_date={partition_date}/{domain}_{file_timestamp}.json"

        record_count = len(payload) if isinstance(payload, list) else 1
        envelope = {
            "metadata": {
                "ingested_at_utc": now_utc.isoformat(),
                "domain": domain,
                "source_url": source_url,
                "record_count": record_count,
            },
            "payload": payload,
        }

        serialized_body = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")

        try:
            self.s3_client.put_object(
                Bucket=self.s3_cfg.bucket_name,
                Key=s3_key,
                Body=serialized_body,
                ContentType="application/json",
            )
            logger.info(
                "Landed %d record(s) to s3://%s/%s",
                record_count,
                self.s3_cfg.bucket_name,
                s3_key,
            )
            return s3_key
        except (BotoCoreError, ClientError) as exc:
            logger.error("S3 upload failed for key '%s': %s", s3_key, exc)
            raise

    def execute_full_ingestion(self) -> dict[str, str]:
        uploaded_keys: dict[str, str] = {}

        fx_endpoint = f"{self.api_cfg.fx_base_url}/latest"
        fx_data = self.fetch_json(fx_endpoint, params={"from": "EUR", "to": "USD,TRY,GBP"})
        uploaded_keys["fx_rates"] = self.upload_to_bronze(
            payload=fx_data,
            domain="fx_rates",
            source_url=fx_endpoint,
        )

        products_endpoint = f"{self.api_cfg.ecommerce_base_url}/products"
        products_data = self.fetch_json(products_endpoint)
        uploaded_keys["products"] = self.upload_to_bronze(
            payload=products_data,
            domain="products",
            source_url=products_endpoint,
        )

        orders_endpoint = f"{self.api_cfg.ecommerce_base_url}/carts"
        orders_data = self.fetch_json(orders_endpoint)
        uploaded_keys["orders"] = self.upload_to_bronze(
            payload=orders_data,
            domain="orders",
            source_url=orders_endpoint,
        )

        return uploaded_keys


def main() -> None:
    s3_cfg = load_s3_config()
    api_cfg = load_api_config()
    ingestor = S3BronzeIngestor(s3_cfg=s3_cfg, api_cfg=api_cfg)
    ingestor.execute_full_ingestion()


if __name__ == "__main__":
    main()