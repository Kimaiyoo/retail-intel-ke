"""Functions for ingesting KAMIS market-price data."""

import io
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone

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
    )
}

class SchemaDriftError(RuntimeError):
    """Raised when KAMIS returns an unexpected response format."""


class CompletenessError(RuntimeError):
    """Raised when a date window cannot be retrieved completely."""


REQUIRED_SOURCE_COLUMNS = {
    "Market",
    "Commodity",
    "Date",
    "Wholesale",
    "Retail",
}


def validate_source_data(df, commodity_name, start, end):
    """Check source columns and ensure returned dates match the requested window."""
    missing = REQUIRED_SOURCE_COLUMNS.difference(df.columns)
    if missing:
        raise SchemaDriftError(
            f"KAMIS export is missing expected columns: {sorted(missing)}"
        )

    if df.empty:
        return

    parsed_dates = pd.to_datetime(df["Date"], errors="coerce").dt.date

    if parsed_dates.isna().any():
        count = int(parsed_dates.isna().sum())
        raise SchemaDriftError(
            f"KAMIS returned {count} row(s) with invalid dates."
        )

    outside_window = (parsed_dates < start) | (parsed_dates > end)
    if outside_window.any():
        examples = parsed_dates[outside_window].head(5).tolist()
        raise SchemaDriftError(
            f"KAMIS returned dates outside the requested window "
            f"{start} to {end}: {examples}"
        )

    # in case of mismatched names warn instead of rejecting data
    actual_names = (
        df["Commodity"].dropna().astype(str).str.strip().unique().tolist()
    )
    normalise = lambda value: re.sub(r"[^a-z0-9]", "", value.lower())
    expected = normalise(commodity_name)
    unexpected = [
        name for name in actual_names if normalise(name) != expected
    ]
    if unexpected:
        log.warning(
            "KAMIS commodity label differs from configured name %r. "
            "Returned label(s): %s. Check the source filter and configured "
            "display name.",
            commodity_name,
            unexpected,
        )


def load_config(path="config/config.yaml"):
    """Load KAMIS configuration from YAML."""
    with open(resolve(path), encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["kamis"]


def validate_config(cfg):
    """Validate settings needed for date-window extraction."""
    start = date.fromisoformat(cfg["start_date"])
    end = date.fromisoformat(cfg["end_date"])

    if start > end:
        raise ValueError("start_date must be on or before end_date.")

    if int(cfg["per_page"]) < 1:
        raise ValueError("per_page must be a positive integer.")

    if not cfg["commodities"]:
        raise ValueError("At least one commodity must be configured.")

    return start, end

def make_session(cfg):
    """Create an HTTP session with retries and SSL verification."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=cfg["max_retries"],
        backoff_factor=cfg["backoff_seconds"],
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    if cfg.get("verify_ssl", True):
        ca_bundle = resolve(cfg["ca_bundle"])

        if not ca_bundle.is_file():
            session.close()
            raise FileNotFoundError(
                f"CA certificate bundle not found: {ca_bundle}"
            )

        session.verify = str(ca_bundle)
    else:
        session.verify = False
        log.warning(
            "TLS certificate verification is disabled for KAMIS."
        )

    return session


def fetch_window(session, cfg, product_id, start, end):
    """Fetch one commodity over an inclusive date range."""
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "per_page": int(cfg["per_page"]),
        "export": "excel",
        "product[]": product_id,
    }

    response = session.get(
        cfg["base_url"],
        params=params,
        timeout=cfg["timeout_seconds"],
    )
    response.raise_for_status()

    raw = response.content
    content_start = raw[:500].lstrip().lower()

    if (
        b"<html" in content_start
        or b"<!doctype html" in content_start
    ):
        raise SchemaDriftError(
            "KAMIS returned HTML instead of an Excel export. "
            f"Commodity={product_id}, dates={start} to {end}, "
            f"URL={response.url}"
        )

    try:
        df = pd.read_excel(io.BytesIO(raw))
    except Exception as exc:
        raise SchemaDriftError(
            "Could not read KAMIS response as Excel. "
            f"Commodity={product_id}, dates={start} to {end}."
        ) from exc

    missing = REQUIRED_SOURCE_COLUMNS.difference(df.columns)
    if missing:
        raise SchemaDriftError(
            "KAMIS Excel export is missing expected columns: "
            f"{sorted(missing)}. Commodity={product_id}, "
            f"dates={start} to {end}."
        )

    return raw, df, response.url

def save_bronze(
    cfg,
    product_id,
    start,
    end,
    raw,
    ingest_ts,
    capped=False,
):
    """Save the original response bytes for audit and reprocessing."""
    run_id = ingest_ts.strftime("%Y-%m-%dT%H%M%S%fZ")

    folder = "diagnostics" if capped else "accepted"
    suffix = "_capped.xlsx" if capped else ".xlsx"

    path = (
        resolve(cfg["bronze_dir"])
        / run_id
        / str(product_id)
        / folder
        / f"{start}_{end}{suffix}"
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)

    return path

def extract_window(
    session,
    cfg,
    product_id,
    commodity_name,
    start,
    end,
    ingest_ts,
    frames,
):
    """
    Extract one date window.

    If the response reaches the configured row limit, split the
    window into smaller, non-overlapping date ranges.
    """
    raw, df, source_url = fetch_window(
        session, cfg, product_id, start, end
    )
    validate_source_data(df, commodity_name, start, end)

    row_limit = int(cfg["per_page"])

    if len(df) >= row_limit:
        diagnostic_path = save_bronze(
            cfg,
            product_id,
            start,
            end,
            raw,
            ingest_ts,
            capped=True,
        )

        log.warning(
            "Window reached the row limit: commodity=%s, "
            "dates=%s to %s, rows=%s, limit=%s. "
            "Saved diagnostic response to %s",
            commodity_name,
            start,
            end,
            len(df),
            row_limit,
            diagnostic_path,
        )

        if start == end:
            raise CompletenessError(
                f"KAMIS returned {len(df)} rows for "
                f"{commodity_name} on {start}, reaching the "
                f"configured limit of {row_limit}. Cannot split "
                "this window further; completeness is unresolved."
            )

        midpoint = start + timedelta(
            days=(end - start).days // 2
        )

        left_end = midpoint
        right_start = midpoint + timedelta(days=1)

        log.info(
            "Splitting %s to %s into %s to %s and %s to %s",
            start,
            end,
            start,
            left_end,
            right_start,
            end,
        )

        time.sleep(cfg["sleep_between_requests"])

        extract_window(
            session,
            cfg,
            product_id,
            commodity_name,
            start,
            left_end,
            ingest_ts,
            frames,
        )

        time.sleep(cfg["sleep_between_requests"])

        extract_window(
            session,
            cfg,
            product_id,
            commodity_name,
            right_start,
            end,
            ingest_ts,
            frames,
        )

        return

    # The response is below the configured limit.
    path = save_bronze(
        cfg,
        product_id,
        start,
        end,
        raw,
        ingest_ts,
    )

    if df.empty:
        log.info(
            "No rows returned for %s between %s and %s.",
            commodity_name,
            start,
            end,
        )
        return

    page_df = df.copy()
    page_df["commodity_id"] = product_id
    page_df["commodity_name"] = commodity_name
    page_df["_ingested_at"] = ingest_ts.isoformat()
    page_df["_source_url"] = source_url
    page_df["_bronze_file"] = str(path)
    page_df["_window_start"] = start.isoformat()
    page_df["_window_end"] = end.isoformat()
    page_df["_request_row_limit"] = row_limit

    frames.append(page_df)

    log.info(
        "Accepted %s rows for %s (%s to %s) -> %s",
        len(page_df),
        commodity_name,
        start,
        end,
        path,
    )

    time.sleep(cfg["sleep_between_requests"])


_PRICE_RE = re.compile(
    r"^\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*(\w+)\s*$"
)


def parse_price(value):
    """Parse a value such as '44.44/Kg' into price and unit."""
    if pd.isna(value):
        return None, None

    match = _PRICE_RE.match(str(value))

    if not match:
        return None, None

    return float(match.group(1)), match.group(2)


def to_silver(df):
    """Clean and standardise KAMIS data for downstream processing."""
    if df.empty:
        return df.copy()

    out = df.copy()

    required_columns = ["Date", "Wholesale", "Retail"]
    missing = [
        col for col in required_columns if col not in out.columns
    ]

    if missing:
        raise ValueError(
            f"KAMIS data is missing required columns: {missing}"
        )

    for col, prefix in (
        ("Wholesale", "wholesale"),
        ("Retail", "retail"),
    ):
        parsed = out[col].map(parse_price)

        out[f"{prefix}_price"] = parsed.map(lambda item: item[0])
        out[f"{prefix}_unit"] = parsed.map(lambda item: item[1])

    out["date"] = pd.to_datetime(
        out["Date"], errors="coerce"
    ).dt.date

    out = out.rename(
        columns={"Supply Volume": "supply_volume"}
    )

    invalid_dates = out["date"].isna().sum()

    if invalid_dates:
        log.warning(
            "%s rows have invalid or missing dates.",
            invalid_dates,
        )

    return out

def run(config_path="config/config.yaml"):
    """Ingest configured KAMIS commodities into Bronze."""
    cfg = load_config(config_path)
    start, end = validate_config(cfg)

    session = make_session(cfg)
    ingest_ts = datetime.now(timezone.utc)

    frames = []
    failures = {}

    try:
        for pid, name in cfg["commodities"].items():
            pid = int(pid)

            log.info(
                "Fetching %s (ID: %s), dates %s to %s",
                name,
                pid,
                start,
                end,
            )

            rows_before = sum(len(frame) for frame in frames)

            try:
                extract_window(
                    session=session,
                    cfg=cfg,
                    product_id=pid,
                    commodity_name=name,
                    start=start,
                    end=end,
                    ingest_ts=ingest_ts,
                    frames=frames,
                )

                rows_after = sum(len(frame) for frame in frames)

                log.info(
                    "Completed %s: %s rows collected",
                    name,
                    rows_after - rows_before,
                )

            except Exception as exc:
                log.exception(
                    "Failed to ingest %s (ID: %s)",
                    name,
                    pid,
                )
                failures[pid] = str(exc)

    finally:
        session.close()

    result = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame()
    )

    if failures:
        raise RuntimeError(
            f"{len(failures)} commodity ingestion(s) failed: "
            f"{failures}. Check the logs and Bronze output."
        )

    return result