from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, count_distinct


def find_all_taxis() -> DataFrame:
    """Find all taxi data."""
    return spark.read.table("samples.nyctaxi.trips")


def create_taxi_unique_trips_view(view_name: str = "taxi_unique_trips") -> str:
    """Count unique trips by date and create a view with the specified name.

    Args:
        view_name: Name of the view to create (default: "taxi_unique_trips")

    Returns:
        The name of the created view
    """
    # Create view using Spark SQL
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
            TO_DATE(tpep_pickup_datetime) AS trip_date,
            COUNT(DISTINCT trip_distance) AS unique_trip_count
        FROM samples.nyctaxi.trips
        GROUP BY TO_DATE(tpep_pickup_datetime)
        ORDER BY trip_date
    """)

    return view_name 


def find_day_with_most_trips(view_name: str) -> dict:
    """Find the day with the most trips from a given view.

    Args:
        view_name: Name of the view containing trip_date and unique_trip_count columns

    Returns:
        Dictionary with 'trip_date' and 'unique_trip_count' keys
    """
    # Read from the view and find the day with the maximum trip count
    result = (
        spark.sql(f"SELECT * FROM {view_name}")
        .orderBy(col("unique_trip_count").desc())
        .limit(1)
        .collect()
    )

    if not result:
        raise ValueError(f"No data found in view {view_name}")

    row = result[0]
    return {
        "trip_date": row.trip_date,
        "unique_trip_count": row.unique_trip_count
    }
