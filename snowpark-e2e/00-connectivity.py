import snowflake.connector
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')


# snowflake connection
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


def main():
    try:
        connection = get_snowflake_connection()

        # create cursor
        cursor = connection.cursor()

        # execute queries
        cursor.execute("select current_role(), current_database(), current_schema(), current_warehouse()")
        context_results = cursor.fetchall()
        print(context_results)

        cursor.execute("select c_custkey, c_name, c_phone, c_mktsegment from "
                       "snowflake_sample_data.tpch_sf1.customer limit 10")
        customer_results = cursor.fetchall()
        print(customer_results)

        # close cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")


if __name__ == '__main__':
    main()
