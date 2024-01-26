import logging
import sys

import snowflake.connector

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%I:%M:%S')


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


def ingest_in_sales(connection) -> None:
    cursor = connection.cursor()

    sql_query = """
    copy into sales_dwh.source.in_sales_order from ( 
        select 
        sales_dwh.source.IN_SALES_ORDER_SEQ.NEXTVAL, 
        t.$1::text as order_id, 
        t.$2::text as customer_name, 
        t.$3::text as mobile_key,
        t.$4::number as order_quantity, 
        t.$5::number as unit_price, 
        t.$6::number as order_valaue,  
        t.$7::text as promotion_code , 
        t.$8::number(10,2)  as final_order_amount,
        t.$9::number(10,2) as tax_amount,
        t.$10::date as order_dt,
        t.$11::text as payment_status,
        t.$12::text as shipping_status,
        t.$13::text as payment_method,
        t.$14::text as payment_provider,
        t.$15::text as mobile,
        t.$16::text as shipping_address,
        metadata$filename as stg_file_name,
        metadata$file_row_number as stg_row_numer,
        metadata$file_last_modified as stg_last_modified
        from 
        @sales_dwh.source.my_internal_stg/source=IN/format=csv/ 
        (                                                             
            file_format => 'sales_dwh.common.my_csv_format'           
        ) t  
    )  on_error = continue  
    """

    cursor.execute(sql_query)
    cursor.close()


def ingest_us_sales(connection) -> None:
    cursor = connection.cursor()

    # your SQL query for ingesting us_sales data
    sql_query = """
    copy into sales_dwh.source.us_sales_order from (                                       
        select                              
        sales_dwh.source.US_SALES_ORDER_SEQ.NEXTVAL, 
        $1:"Order ID"::text as orde_id,   
        $1:"Customer Name"::text as customer_name,
        $1:"Mobile Model"::text as mobile_key,
        to_number($1:"Quantity") as quantity,
        to_number($1:"Price per Unit") as unit_price,
        to_decimal($1:"Total Price") as total_price,
        $1:"Promotion Code"::text as promotion_code,
        $1:"Order Amount"::number(10,2) as order_amount,
        to_decimal($1:"Tax") as tax,
        $1:"Order Date"::date as order_dt,
        $1:"Payment Status"::text as payment_status,
        $1:"Shipping Status"::text as shipping_status,
        $1:"Payment Method"::text as payment_method,
        $1:"Payment Provider"::text as payment_provider,
        $1:"Phone"::text as phone,
        $1:"Delivery Address"::text as shipping_address,
        metadata$filename as stg_file_name,
        metadata$file_row_number as stg_row_numer,
        metadata$file_last_modified as stg_last_modified
        from                                
            @sales_dwh.source.my_internal_stg/source=US/format=parquet/
            (
                file_format => 'sales_dwh.common.my_parquet_format'
            ) 
            
    ) on_error = continue
    """

    cursor.execute(sql_query)
    cursor.close()


def ingest_fr_sales(connection) -> None:
    cursor = connection.cursor()

    # your SQL query for ingesting fr_sales data
    sql_query = """
        copy into sales_dwh.source.fr_sales_order from (                                                       
            select                                              
            sales_dwh.source.FR_SALES_ORDER_SEQ.NEXTVAL,         
            $1:"Order ID"::text as orde_id,                   
            $1:"Customer Name"::text as customer_name,          
            $1:"Mobile Model"::text as mobile_key,              
            to_number($1:"Quantity") as quantity,               
            to_number($1:"Price per Unit") as unit_price,       
            to_decimal($1:"Total Price") as total_price,        
            $1:"Promotion Code"::text as promotion_code,        
            $1:"Order Amount"::number(10,2) as order_amount,    
            to_decimal($1:"Tax") as tax,                        
            $1:"Order Date"::date as order_dt,                  
            $1:"Payment Status"::text as payment_status,        
            $1:"Shipping Status"::text as shipping_status,      
            $1:"Payment Method"::text as payment_method,        
            $1:"Payment Provider"::text as payment_provider,    
            $1:"Phone"::text as phone,                          
            $1:"Delivery Address"::text as shipping_address ,    
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified
            from                                                
            @sales_dwh.source.my_internal_stg/source=FR/format=json/
            (
                file_format => 'sales_dwh.common.my_json_format'
            ) 
    ) on_error=continue
    """

    cursor.execute(sql_query)
    cursor.close()


def main():
    # get the Snowflake connection
    connection = get_snowflake_connection()

    # ingest in_sales data
    ingest_in_sales(connection)

    # ingest us_sales data
    ingest_us_sales(connection)

    # ingest fr_sales data
    ingest_fr_sales(connection)

    # close the connection
    connection.close()


if __name__ == '__main__':
    main()
