import os
import snowflake.connector
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%I:%M:%S')


def get_snowflake_connection():
    connection_parameters = {
        "account": "nwbamyi-uw79442",
        "user": "snowpark_user",
        "password": "Test@12$4",
        "role": "SYSADMIN",
        "database": "SALES_DWH",
        "schema": "SOURCE",
        "warehouse": "SNOWPARK_ETL_WH"
    }
    try:
        # creating snowflake connection object
        return snowflake.connector.connect(**connection_parameters)
    except Exception as e:
        logging.error(f"Error creating Snowflake connection: {e}")
        raise


def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store file paths
    partition_dir = []
    print(directory)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    return file_name, partition_dir, local_file_path


def main():
    # Specify the directory path to traverse
    directory_path = '../sales/'
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')
    stage_location = 'sales_dwh.source.my_internal_stg'

    try:
        # Create a Snowflake connection
        connection = get_snowflake_connection()

        # Create a cursor
        cursor = connection.cursor()

        # Set the current role, database, and schema
        cursor.execute("USE ROLE SYSADMIN")
        cursor.execute("USE DATABASE sales_dwh")
        cursor.execute("USE SCHEMA source")

        # # Stage CSV files
        csv_index = 0
        for _ in csv_file_name:
            stage_location_csv = f"{stage_location}/{csv_partition_dir[csv_index]}"
            cursor.execute(f"PUT file://{csv_local_file_path[csv_index]} @{stage_location_csv}")
            csv_index += 1

        # Stage Parquet files
        parquet_index = 0
        for _ in parquet_file_name:
            stage_location_parquet = f"{stage_location}/{parquet_partition_dir[parquet_index]}"
            cursor.execute(f"PUT file://{parquet_local_file_path[parquet_index]} @{stage_location_parquet}")
            parquet_index += 1

        # Stage JSON files
        json_index = 0
        for _ in json_file_name:
            stage_location_json = f"{stage_location}/{json_partition_dir[json_index]}"
            cursor.execute(f"PUT file://{json_local_file_path[json_index]} @{stage_location_json}")
            json_index += 1

        stage_location_exchange_rate = f"{stage_location}/exchange"
        cursor.execute(f"PUT file://../exchange-rate-data.csv @{stage_location_exchange_rate}")

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
    finally:
        # Close cursor and connection
        cursor.close()
        connection.close()


if __name__ == '__main__':
    main()
