import logging
from pathlib import Path

import pandas as pd
import yaml
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from retail_intel_ke.paths import resolve
from retail_intel_ke.spark.session import get_spark


log = logging.getLogger(__name__)

# Excel column -> raw field name. Order defines the tuple layout.
SOURCE_COLUMNS = {
    "Market": "market",
    "Commodity": "commodity",
    "Classification": "classification",
    "Grade": "grade",
    "Sex": "sex",
    "Wholesale": "wholesale",
    "Retail": "retail",
    "Supply Volume": "supply_volume",
    "County": "county",
    "Date": "date",
}

REQUIRED_SOURCE_COLUMNS = {
    "Market",
    "Commodity",
    "Date",
    "Wholesale",
    "Retail",
}

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SOURCE_COLUMNS.values()]
    + [
        StructField("commodity_id", StringType(), True),
        StructField("_run_id", StringType(), True),
        StructField("_bronze_file", StringType(), True),
        StructField("_row_in_file", LongType(), True),
    ]
)

# Identifies one observation
NATURAL_KEY = [
    "commodity_id",
    "classification",
    "grade",
    "sex",
    "market",
    "county",
    "price_date",
]

VALUE_COLS = [
    "wholesale_price",
    "wholesale_unit",
    "retail_price",
    "retail_unit",
    "supply_volume",
]

# 
PRICE_RE = r"^\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*(\S.*?)\s*$"

SILVER_COLUMNS = [
    "commodity_id",
    "commodity_name",
    "classification",
    "grade",
    "sex",
    "market",
    "county",
    "price_date",
    "price_month",
    "wholesale_price",
    "wholesale_unit",
    "retail_price",
    "retail_unit",
    "supply_volume",
    "n_source_rows",
    "reports_in_key",
    "_run_id",
    "_bronze_file",
    "_silver_processed_at",
]

QUARANTINE_COLUMNS = [
    "commodity_id",
    "commodity_name",
    "market",
    "county",
    "date_raw",
    "wholesale_raw",
    "retail_raw",
    "_reject_reason",
    "_run_id",
    "_bronze_file",
    "_row_in_file",
]


class BronzeSchemaError(RuntimeError):
    """Raised when a Bronze file does not have the expected columns."""

def load_config(path="config/config.yaml"):
    """Load Spark job configuration from YAML."""
    with open(resolve(path), encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["spark"]

# Reading Bronze
def find_bronze_files(bronze_dir):
    return sorted(Path(bronze_dir).glob("*/*/accepted/*.xlsx"))


def read_bronze(bronze_dir):
    """Read with pandas. Handed to spark as strings"""
    rows = []

    for path in find_bronze_files(bronze_dir):
        df = pd.read_excel(path, dtype=str) 

        missing = REQUIRED_SOURCE_COLUMNS.difference(df.columns)
        if missing:
            raise BronzeSchemaError(
                f"{path} is missing columns: {sorted(missing)}"
            )

        if df.empty:
            continue

        for column in SOURCE_COLUMNS:
            if column not in df.columns:
                df[column] = None

        run_id = path.parents[2].name
        commodity_id = path.parents[1].name

        records = df[list(SOURCE_COLUMNS)].itertuples(index=False, name=None)
        for position, record in enumerate(records):
            values = tuple(None if pd.isna(v) else str(v) for v in record)
            rows.append(
                values + (commodity_id, run_id, str(path), position)
            )

    return rows

# Transformations

def _blank_to_null(col):
    trimmed = F.trim(col)
    return F.when(trimmed.isin("", "-"), F.lit(None)).otherwise(trimmed)


def _extract_price(col):
    """Return (cleaned text, numeric price, unit) columns."""
    cleaned = F.regexp_replace(_blank_to_null(col), ",", "")
    value = F.regexp_extract(cleaned, PRICE_RE, 1)
    unit = F.regexp_extract(cleaned, PRICE_RE, 2)

    return (
        cleaned,
        F.when(value != "", value.cast("double")),
        F.when(unit != "", unit),
    )


def to_typed(raw):
    """Cast raw strings to typed columns and parse the price fields."""
    wholesale_text, wholesale_price, wholesale_unit = _extract_price(
        F.col("wholesale")
    )
    retail_text, retail_price, retail_unit = _extract_price(F.col("retail"))

    supply = F.regexp_replace(_blank_to_null(F.col("supply_volume")), ",", "")

    return raw.select(
        F.col("commodity_id").cast("int").alias("commodity_id"),
        F.trim(F.col("commodity")).alias("commodity_name"),
        F.trim(F.col("classification")).alias("classification"),
        F.trim(F.col("grade")).alias("grade"),
        F.trim(F.col("sex")).alias("sex"),
        _blank_to_null(F.col("market")).alias("market"),
        _blank_to_null(F.col("county")).alias("county"),
        F.to_date(
            F.substring(F.trim(F.col("date")), 1, 10), "yyyy-MM-dd"
        ).alias("price_date"),
        wholesale_price.alias("wholesale_price"),
        wholesale_unit.alias("wholesale_unit"),
        retail_price.alias("retail_price"),
        retail_unit.alias("retail_unit"),
        supply.cast("double").alias("supply_volume"),
        # A value was present but did not match 'number/unit'.
        (wholesale_text.isNotNull() & wholesale_price.isNull()).alias(
            "_wholesale_unparseable"
        ),
        (retail_text.isNotNull() & retail_price.isNull()).alias(
            "_retail_unparseable"
        ),
        F.col("date").alias("date_raw"),
        F.col("wholesale").alias("wholesale_raw"),
        F.col("retail").alias("retail_raw"),
        F.col("_run_id"),
        F.col("_bronze_file"),
        F.col("_row_in_file"),
    )


def dedupe_latest(df):
    """Keep most recent run'r report, collapse exact copies"""
    by_key = Window.partitionBy(*NATURAL_KEY)
    latest_run = df.withColumn(
        "_run_rank",
        F.dense_rank().over(by_key.orderBy(F.col("_run_id").desc())),
    ).filter(F.col("_run_rank") == 1)
 
    copies = Window.partitionBy(*NATURAL_KEY, *VALUE_COLS)
    first_copy_first = copies.orderBy(
        F.col("_bronze_file"), F.col("_row_in_file")
    )
 
    return (
        latest_run.withColumn(
            "n_source_rows", F.count(F.lit(1)).over(copies)
        )
        .withColumn("_copy_rank", F.row_number().over(first_copy_first))
        .filter(F.col("_copy_rank") == 1)
        .withColumn("reports_in_key", F.count(F.lit(1)).over(by_key))
        .drop("_run_rank", "_copy_rank")
    )
 

def count_key_conflicts(typed):
    # if same key appears twice in one run _> report
    return (
        typed.filter(F.col("price_date").isNotNull())
        .groupBy(*NATURAL_KEY, "_run_id")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.size(
                F.collect_set(
                    F.struct(
                        "wholesale_price", "retail_price", "supply_volume"
                    )
                )
            ).alias("variants"),
        )
        .filter((F.col("rows") > 1) & (F.col("variants") > 1))
        .count()
    )


def with_reject_reason(df):
    """Add ``_reject_reason``: NULL for valid rows, else the first failure."""
    reason = (
        F.when(F.col("price_date").isNull(), "invalid_date")
        # One day of tolerance: Kenya (UTC+3) is ahead of the UTC session
        # clock, so a record dated 'today' can look a few hours early.
        .when(
            F.col("price_date") > F.date_add(F.current_date(), 1),
            "future_date",
        )
        .when(
            F.col("_wholesale_unparseable") | F.col("_retail_unparseable"),
            "unparseable_price",
        )
        .when(
            F.col("wholesale_price").isNull() & F.col("retail_price").isNull(),
            "no_price",
        )
        .when(
            (F.col("wholesale_price") <= 0) | (F.col("retail_price") <= 0),
            "non_positive_price",
        )
        .when(
            F.col("market").isNull() | F.col("county").isNull(),
            "missing_market_or_county",
        )
    )

    return df.withColumn("_reject_reason", reason)


def split_valid_and_rejects(typed):
    """Deduplicate, validate, and split into (silver, quarantine)."""
    dated = typed.filter(F.col("price_date").isNotNull())
    undated = typed.filter(F.col("price_date").isNull())

    # Undated rows to validation then rejected (can't be keyed)
    undated = undated.withColumn(
        "n_source_rows", F.lit(1).cast("long")
    ).withColumn("reports_in_key", F.lit(1).cast("long"))
    combined = dedupe_latest(dated).unionByName(undated)
    flagged = with_reject_reason(combined)

    silver = (
        flagged.filter(F.col("_reject_reason").isNull())
        .withColumn("price_month", F.date_format("price_date", "yyyy-MM"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )
    quarantine = flagged.filter(F.col("_reject_reason").isNotNull()).select(
        *QUARANTINE_COLUMNS
    )

    return silver, quarantine


def transform(raw):
    """Raw string DataFrame -> (silver, quarantine) DataFrames."""
    return split_valid_and_rejects(to_typed(raw))


# Job entry point
def run(config_path="config/config.yaml", spark=None):
    """Build the KAMIS Silver and quarantine datasets from Bronze."""
    cfg = load_config(config_path)

    owns_session = spark is None
    if owns_session:
        spark = get_spark(
            app_name=cfg.get("app_name", "retail_intel_ke"),
            master=cfg.get("master", "local[*]"),
            shuffle_partitions=cfg.get("shuffle_partitions", 8),
        )

    try:
        bronze_dir = resolve(cfg["kamis_bronze_dir"])
        rows = read_bronze(bronze_dir)

        if not rows:
            raise FileNotFoundError(
                f"No KAMIS rows found under {bronze_dir}. "
                "Run scripts/ingest_kamis.py first."
            )

        raw = spark.createDataFrame(rows, RAW_SCHEMA)
        typed = to_typed(raw).cache()

        rows_read = typed.count()
        conflicts = count_key_conflicts(typed)
        silver, quarantine = split_valid_and_rejects(typed)
        silver = silver.cache()
        quarantine = quarantine.cache()

        silver_path = str(resolve(cfg["kamis_silver_dir"]))
        quarantine_path = str(resolve(cfg["kamis_quarantine_dir"]))

        (
            silver.repartition("price_month")
            .write.mode("overwrite")
            .partitionBy("price_month")
            .parquet(silver_path)
        )
        quarantine.write.mode("overwrite").parquet(quarantine_path)

        silver_rows = silver.count()
        reject_counts = {
            row["_reject_reason"]: row["count"]
            for row in quarantine.groupBy("_reject_reason").count().collect()
        }
        unit_counts = (
            silver.groupBy("commodity_name", "retail_unit", "wholesale_unit")
            .count()
            .orderBy("commodity_name", F.desc("count"))
            .collect()
        )

        summary = {
            "rows_read": rows_read,
            "silver_rows": silver_rows,
            # exact copies collapsed
            "rows_collapsed": (
                rows_read - silver_rows - sum(reject_counts.values())
            ),
            "rejected": reject_counts,
            "within_run_key_conflicts": conflicts,
            "silver_path": silver_path,
            "quarantine_path": quarantine_path,
        }

        log.info("Read %s Bronze rows", rows_read)
        log.info(
            "Silver: %s rows (%s older duplicates removed) -> %s",
            silver_rows,
            summary["rows_collapsed"],
            silver_path,
        )
        log.info(
            "Quarantine: %s rows %s -> %s",
            sum(reject_counts.values()),
            reject_counts,
            quarantine_path,
        )
        if conflicts:
            log.warning(
                "%s keys have several distinct reports in one run; "
                "all were kept(see reports_in_key)",
                conflicts,
            )
        for row in unit_counts:
            log.info(
                "Unit check: %s | retail=%s | wholesale=%s | %s rows",
                row["commodity_name"],
                row["retail_unit"],
                row["wholesale_unit"],
                row["count"],
            )

        return summary

    finally:
        if owns_session:
            spark.stop()