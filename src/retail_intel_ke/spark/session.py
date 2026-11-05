from pyspark.sql import SparkSession


def get_spark(
    app_name="retail_intel_ke",
    master="local[*]",
    shuffle_partitions=8,
):

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # no errors raised - failed parse -> NULL then row -> rejected(quarantine)
        .config("spark.sql.ansi.enabled", "false")
        # Parse dates strictly with the new parser; no legacy fallbacks.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )