from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from airflow import DAG
import logging
import requests
import json
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def get_spark_session():
    if 'spark' not in globals():
        globals()['spark'] = SparkSession.builder \
            .appName("BreweryDataProcessing") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
    return globals()['spark']

def get_breweries_save_bronze_from_api():
    try:
        url = "https://api.openbrewerydb.org/breweries"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        print(f"API Response: {response.text}")

        data = response.json()
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    for key in item:
                        if item.get(key) is None:
                            item[key] = ""
        elif isinstance(data, dict):
            for key in data:
                if data.get(key) is None:
                    data[key] = ""
        
                    
        save_breweries_bronze_zone(data)

    except requests.exceptions.RequestException as e:
        print(f"Error getting data from the API: {e}")
        return []
    
def save_breweries_bronze_zone(data):
    try:
        output_dir = f"/output/data/bronze/brewery/{datetime.now().strftime('%Y/%m')}/"
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, "breweries.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"Dados salvos com sucesso em {file_path}")
                        
        return data
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
       
def save_breweries_silver_zone(data):
    try:
        logging.info("Iniciando o SparkSession")
        
        spark = get_spark_session()
       
        now = datetime.now()
        year = now.year
        month = str(now.month).zfill(2)

        folder_path = f"/output/data/bronze/brewery/{year}/{month}"
        file_path = os.path.join(folder_path, "breweries.json")
        
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder '{folder_path}' doens't exist.")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File'{file_path}' doens't exist.")
                        
                        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True)
        ])

        df = spark.read.json(folder_path, schema=schema, multiLine=True)
        
        output_path = f"/output/data/silver/brewery/"
        
        df_cleaned = df \
            .filter(col("country").isNotNull()) \
            .filter(col("state_province").isNotNull()) \
            .filter(col("city").isNotNull())

        df_cleaned.write.mode("overwrite").partitionBy("country", "state_province", "city").parquet(output_path)
        df_cleaned.show(5)
        print(f"Dados salvos em {output_path}")

    except ValueError as e:
        print(f"Data format error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            
def save_breweries_gold_zone():
    try:
        spark = get_spark_session()
        
        input_path = "/output/data/silver/brewery"
        
        df = spark.read.parquet(input_path)
        
        df_aggregated = df.groupBy("country", "state_province", "brewery_type") \
                         .agg(count("id").alias("brewery_count"))
        
        output_path = "/output/data/gold/brewery_aggregation"
        df_aggregated.write.mode("overwrite").parquet(output_path)
        df_aggregated.show()
        
        print(f"Dados agregados salvos em {output_path}")
    
    except Exception as e:
        print(f"Erro inesperado: {e}")
        raise
    
    finally:
        if 'spark' in locals():
            spark.stop()

with DAG(
    dag_id="brewery_process_dag",
    default_args=default_args,
    description="Collecting and saving brewery data",
    schedule_interval="30 12 * * *",
    start_date=datetime(2025, 2, 9),
    catchup=False,
) as dag_brewery_data:

    get_breweries_save_bronze = PythonOperator(
        task_id="get_breweries_save_bronze",
        python_callable=get_breweries_save_bronze_from_api
    )
    
    save_breweries_silver_zone_task = PythonOperator(
        task_id="save_breweries_silver_zone",
        python_callable=save_breweries_silver_zone,
        op_args=["{{ task_instance.xcom_pull(task_ids='get_breweries_save_bronze') }}"]
    )
    
    save_breweries_gold_brewery_aggregation = PythonOperator(
        task_id="save_breweries_gold_zone",
        python_callable=save_breweries_gold_zone,
    )

    start_dag = EmptyOperator(task_id='start_dag')

    end_dag = EmptyOperator(task_id='end_dag')

    start_dag >> get_breweries_save_bronze >> save_breweries_silver_zone_task >> save_breweries_gold_brewery_aggregation >> end_dag
