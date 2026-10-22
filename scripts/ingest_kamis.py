
import logging

from retail_intel_ke.ingestion.kamis import run, to_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

if __name__ == "__main__":
    raw_df = run()
    silver_df = to_silver(raw_df)

    print(silver_df.head(10))
    print(f"Rows ingested: {len(raw_df)}")
