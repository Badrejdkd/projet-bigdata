import json
import boto3
from kafka import KafkaProducer
from datetime import datetime, timezone
from collections import defaultdict


class MinIOPipeline:

    def open_spider(self, spider):
        try:
            self.client = boto3.client(
                "s3",
                endpoint_url="http://localhost:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin123",
            )
            self.buffer = defaultdict(list)
            self._ensure_bucket("datalake")
            spider.logger.info("[MinIO] Connexion OK")
        except Exception as e:
            spider.logger.warning(f"[MinIO] Connexion échouée : {e}")
            self.client = None
            self.buffer = defaultdict(list)

    def _ensure_bucket(self, bucket):
        existing = [b["Name"] for b in self.client.list_buckets()["Buckets"]]
        if bucket not in existing:
            self.client.create_bucket(Bucket=bucket)

    def process_item(self, item, spider):
        key = (item.get("source", "unknown"), item.get("categorie", "unknown"))
        self.buffer[key].append(dict(item))
        return item

    def close_spider(self, spider):
        if self.client is None:
            spider.logger.warning("[MinIO] Pas de client, données non sauvegardées")
            return

        now = datetime.now(timezone.utc)
        path_date = f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}"

        for (source, categorie), items in self.buffer.items():
            source_clean = source.replace(".", "_").replace(" ", "_")
            key = f"bronze/books/{source_clean}/{path_date}/{categorie}.json"
            body = "\n".join(json.dumps(item, ensure_ascii=False) for item in items)
            self.client.put_object(
                Bucket="datalake",
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            spider.logger.info(f"[MinIO] {len(items)} items → {key}")


class KafkaPipeline:

    def open_spider(self, spider):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=3,
            )
            spider.logger.info("[Kafka] Connexion OK")
        except Exception as e:
            spider.logger.warning(f"[Kafka] Connexion échouée : {e}")
            self.producer = None

    def process_item(self, item, spider):
        if self.producer:
            self.producer.send("books.raw.events", value=dict(item))
        return item

    def close_spider(self, spider):
        if self.producer:
            self.producer.flush()
            self.producer.close()