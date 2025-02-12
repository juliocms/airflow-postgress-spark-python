from unittest.mock import patch, MagicMock, mock_open
import unittest
import json
from airflow.dags.brewery_process import (
    get_breweries_save_bronze_from_api,
    save_breweries_bronze_zone,
    save_breweries_silver_zone,
    save_breweries_gold_zone,
)
from pyspark import SparkContext
from unittest.mock import MagicMock, patch

class TestBreweryPipeline(unittest.TestCase):
    
    @patch("requests.get")
    @patch("airflow.dags.brewery_process.save_breweries_bronze_zone")
    def test_get_breweries_save_bronze_from_api(self, mock_save_bronze, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "1", "name": "Test Brewery", "city": "Test City"}]
        mock_requests.return_value = mock_response
        
        get_breweries_save_bronze_from_api()
        
        mock_requests.assert_called_once_with("https://api.openbrewerydb.org/breweries", timeout=10)
        mock_save_bronze.assert_called_once()
    
    @patch("builtins.open", new_callable=lambda: mock_open())
    @patch("os.makedirs")
    def test_save_breweries_bronze_zone(self, mock_makedirs, mock_open_file):
        test_data = [{"id": "1", "name": "Test Brewery", "city": "Test City"}]
        save_breweries_bronze_zone(test_data)
        mock_makedirs.assert_called_once_with('/output/data/bronze/brewery/2025/02/', exist_ok=True)  # Ajuste o caminho conforme necessário
        mock_open_file.assert_called_once_with('/output/data/bronze/brewery/2025/02/breweries.json', 'w', encoding='utf-8')  # Ajuste o caminho do arquivo
        handle = mock_open_file()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        assert written_data == json.dumps(test_data, indent=4)
    
    @patch("airflow.dags.brewery_process.get_spark_session")
    @patch("os.path.exists")
    @patch("airflow.dags.brewery_process.SparkSession")
    def test_save_breweries_silver_zone(self, mock_spark_session, mock_path_exists, mock_get_spark_session):
        mock_path_exists.return_value = True
        mock_spark = MagicMock()
        mock_get_spark_session.return_value = mock_spark
        mock_spark.read.json.return_value = MagicMock()
        SparkContext._active_spark_context = MagicMock()
        save_breweries_silver_zone([])
        SparkContext._active_spark_context = None
    
    @patch("airflow.dags.brewery_process.get_spark_session")
    @patch("airflow.dags.brewery_process.SparkSession")
    def test_save_breweries_gold_zone(self, mock_spark_session, mock_get_spark_session):
        mock_spark = MagicMock()
        mock_get_spark_session.return_value = mock_spark
        mock_spark.read.parquet.return_value = MagicMock()
        SparkContext._active_spark_context = MagicMock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark
        save_breweries_gold_zone()
        SparkContext._active_spark_context = None
    
if __name__ == "__main__":
    unittest.main()
