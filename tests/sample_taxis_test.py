from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from doan_dabs_test import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5


def test_create_taxi_unique_trips_view():
    # Create the view
    taxis.create_taxi_unique_trips_view()

    # Verify the view was created and has data
    result = spark.sql("SELECT * FROM taxi_unique_trips")
    assert result.count() > 0

    # Verify the schema has the expected columns
    columns = result.columns
    assert "trip_date" in columns
    assert "unique_trip_count" in columns

    # Verify the data is sorted by date
    dates = [row.trip_date for row in result.collect()]
    assert dates == sorted(dates)


def test_find_day_with_most_trips():
    # First create the view
    taxis.create_taxi_unique_trips_view()

    # Find the day with the most trips
    result = taxis.find_day_with_most_trips("taxi_unique_trips")

    # Verify the result has the expected structure
    assert "trip_date" in result
    assert "unique_trip_count" in result

    # Verify the values are not None
    assert result["trip_date"] is not None
    assert result["unique_trip_count"] is not None

    # Verify the count is a positive number
    assert result["unique_trip_count"] > 0

    # Verify this is actually the maximum by checking against all rows
    all_data = spark.sql("SELECT * FROM taxi_unique_trips").collect()
    max_count = max(row.unique_trip_count for row in all_data)
    assert result["unique_trip_count"] == max_count


def test_create_taxi_unique_trips_view_with_custom_name():
    # Create the view with a custom name
    custom_view_name = "test_taxi_trips_custom"
    created_view = taxis.create_taxi_unique_trips_view(custom_view_name)

    # Verify the correct view name was returned
    assert created_view == custom_view_name

    # Verify the view was created and has data
    result = spark.sql(f"SELECT * FROM {custom_view_name}")
    assert result.count() > 0

    # Verify the schema has the expected columns
    columns = result.columns
    assert "trip_date" in columns
    assert "unique_trip_count" in columns

    # Test that find_day_with_most_trips works with the custom view name
    max_day_result = taxis.find_day_with_most_trips(custom_view_name)
    assert "trip_date" in max_day_result
    assert "unique_trip_count" in max_day_result
