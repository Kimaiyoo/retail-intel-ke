"""Functions for ingesting KNBS Consumer Price Index data ."""

import logging
import math
from datetime import date, datetime, timezone

import pandas as pd
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from retail_intel_ke.paths import resolve


log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://opendata.knbs.or.ke/beta/databrowser/",
}

# Request body the KNBS Data Browser sends(monthly frequency)
PAYLOAD = [
    {"id": "FREQ", "filterValues": ["M"], "type": "CodeValues", "period": 0}
]

# The dimension order decides how value keys map to cells, so a change
# in this order is treated as schema drift.
EXPECTED_DIMENSIONS = ["FREQ", "ITEM_PRICE", "UNIT_MEASURE", "TIME_PERIOD"]

# KNBS publishes with a lag of a few months; warn if the series is older.
MAX_EXPECTED_LAG_DAYS = 150


class SchemaDriftError(RuntimeError):
    """Raised when KNBS returns an unexpected response format."""


def load_config(path="config/config.yaml"):
    """Load KNBS configuration from YAML."""
    with open(resolve(path), encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["knbs"]


def make_session(cfg):
    """Create an HTTP session with retries."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=cfg.get("max_retries", 4),
        backoff_factor=cfg.get("backoff_seconds", 2),
        status_forcelist=(429, 500, 502, 503, 504),
        # data request is POST, but ony reads data
        allowed_methods=("GET", "POST"),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    return session


def validate_response(js):
    """Check that the JSON-stat response has the shape we parse."""
    if not isinstance(js, dict):
        raise SchemaDriftError("KNBS response is not a JSON object.")

    missing = {"id", "size", "value", "dimension"}.difference(js)
    if missing:
        raise SchemaDriftError(
            f"KNBS response is missing keys: {sorted(missing)}"
        )

    if js["id"] != EXPECTED_DIMENSIONS:
        raise SchemaDriftError(
            f"Unexpected dimensions {js['id']}; "
            f"expected {EXPECTED_DIMENSIONS}."
        )

    if not js["value"]:
        raise SchemaDriftError("KNBS response contains no values.")

    for dim, size in zip(js["id"], js["size"]):
        found = len(js["dimension"][dim]["category"]["index"])
        if found != size:
            raise SchemaDriftError(
                f"Dimension {dim} lists {found} categories "
                f"but size says {size}."
            )

    total_cells = math.prod(js["size"])
    out_of_range = [
        key
        for key in js["value"]
        if not str(key).isdigit() or int(key) >= total_cells
    ]
    if out_of_range:
        raise SchemaDriftError(
            f"{len(out_of_range)} value keys fall outside the "
            f"{total_cells}-cell grid: {out_of_range[:5]}"
        )


def fetch_cpi(session, cfg):
    """Fetch the CPI dataset. Returns raw bytes, parsed JSON and URL."""
    response = session.post(
        cfg["data_url"],
        json=PAYLOAD,
        timeout=cfg.get("timeout_seconds", 60),
    )
    response.raise_for_status()

    try:
        js = response.json()
    except ValueError as exc:
        raise SchemaDriftError(
            f"KNBS did not return JSON. URL={response.url}"
        ) from exc

    validate_response(js)

    return response.content, js, response.url


def save_bronze(cfg, raw, ingest_ts):
    """Save the original response bytes for audit and reprocessing."""
    run_id = ingest_ts.strftime("%Y-%m-%dT%H%M%S%fZ")

    path = resolve(cfg["bronze_dir"]) / run_id / "cpi.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)

    return path


def _ordered_codes(index):
    """Category codes in position order (JSON-stat allows list or dict)."""
    if isinstance(index, dict):
        return sorted(index, key=index.get) #type:ignore

    return list(index)

def jsonstat_to_df(js):
    """Flatten a JSON-stat dataset into one row per observation.
    """
    ids, sizes = js["id"], js["size"]

    codes = {}
    labels = {}
    for dim in ids:
        category = js["dimension"][dim]["category"]
        codes[dim] = _ordered_codes(category["index"])
        labels[dim] = category.get("label", {})

    rows = []
    for key, value in js["value"].items():
        if value is None:
            continue

        remaining = int(key)
        positions = []
        for size in reversed(sizes):
            positions.append(remaining % size)
            remaining //= size
        positions.reverse()

        row = {}
        for dim, position in zip(ids, positions):
            code = codes[dim][position]
            row[dim.lower()] = code
            row[f"{dim.lower()}_label"] = labels[dim].get(code, code)

        row["cpi_index"] = value
        rows.append(row)

    return pd.DataFrame(rows)


def to_silver(df):
    """Clean and standardize CPI data for downstream processing."""
    if df.empty:
        return df.copy()

    out = df.copy()

    period_start = pd.to_datetime(
        out["time_period"] + "-01", format="%Y-%m-%d", errors="coerce"
    )
    invalid_periods = int(period_start.isna().sum())
    if invalid_periods:
        raise SchemaDriftError(
            f"{invalid_periods} rows have an unparseable time period."
        )
    out["period_start"] = period_start.dt.date

    out["cpi_index"] = pd.to_numeric(out["cpi_index"], errors="coerce")
    invalid = out["cpi_index"].isna() | (out["cpi_index"] <= 0)
    if invalid.any():
        log.warning(
            "Dropping %s rows with a missing or non-positive CPI value.",
            int(invalid.sum()),
        )
        out = out[~invalid]

    if out.empty:
        return out

    key = ["item_price", "period_start"]
    duplicate_count = int(out.duplicated(subset=key, keep=False).sum())

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate rows /n CPI (item,month) keys"
        )

    latest = max(out["period_start"])
    lag_days = (date.today() - latest).days
    log.info("Latest CPI period: %s (%s days ago)", latest, lag_days)
    if lag_days > MAX_EXPECTED_LAG_DAYS:
        log.warning(
            "CPI series looks stale: latest period %s is %s days old.",
            latest,
            lag_days,
        )

    return out.reset_index(drop=True)


def run(config_path="config/config.yaml"):
    """Ingest the KNBS CPI dataset into Bronze and return raw rows."""
    cfg = load_config(config_path)
    ingest_ts = datetime.now(timezone.utc)

    session = make_session(cfg)
    try:
        raw, js, source_url = fetch_cpi(session, cfg)
    finally:
        session.close()

    path = save_bronze(cfg, raw, ingest_ts)

    df = jsonstat_to_df(js)
    if df.empty:
        raise SchemaDriftError("KNBS response had no usable values.")

    expected_cells = math.prod(js["size"])
    if len(df) < expected_cells:
        log.warning(
            "Sparse response: %s of %s cells have values.",
            len(df),
            expected_cells,
        )

    df["_ingested_at"] = ingest_ts.isoformat()
    df["_source_url"] = source_url
    df["_bronze_file"] = str(path)

    log.info("Accepted %s CPI rows -> %s", len(df), path)

    return df