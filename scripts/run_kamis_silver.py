import logging

from retail_intel_ke.spark.kamis_silver import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logging.getLogger("py4j").setLevel(logging.WARNING)

if __name__ == "__main__":
    summary = run()
    print(summary)