import pandas as pd
import ssl
import re
import os
from urllib import request, error
from elasticsearch import Elasticsearch, BadRequestError
from elasticsearch.helpers import bulk
from logger import MyLogger
from omegaconf import OmegaConf

# Get the directory where the script is located
this_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory of the script directory
parent_dir = os.path.dirname(this_dir)

# Path to the configuration file
config_path = os.path.join(parent_dir, "conf", "config.yaml")

# Config
cfg = OmegaConf.load(config_path)
# Logger
my_logger = MyLogger().get_logger()


def download_file(url: str, filename: str) -> None:
    try:
        my_logger.info(f"Current working directory: {os.getcwd()}")
        my_logger.info(f"Downloading from URL: {url}")

        # Handle SSL verification issues
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with request.urlopen(url, context=context) as response:
            my_logger.info(f"Response status: {response.status}")
            data = response.read()

            # # Ensure directory exists
            # if os.path.dirname(filename):
            #     os.makedirs(os.path.dirname(filename), exist_ok=True)

            with open(filename, "wb") as f:
                f.write(data)

        if os.path.exists(filename):
            my_logger.info(f"Downloaded {filename} successfully")
        else:
            my_logger.error(f"Failed to download {filename}")

    except error.HTTPError as e:
        my_logger.error(f"HTTP error: {e.code} - {e.reason}")
    except error.URLError as e:
        my_logger.error(f"URL error: {e.reason}")
    except Exception as e:
        my_logger.error(f"Unexpected error: {str(e)}")

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values (NaN, N/A) in dataframe"""
    # Replace "__NA__" and "N/A" with NaN first to standardize missing values
    df.replace(["__NA__", "N/A"], pd.NA, inplace=True)

    # Separate numerical and string columns
    num_cols = df.select_dtypes(include=['number']).columns
    str_cols = df.select_dtypes(include=['object', 'string']).columns

    # Fill NaNs accordingly
    df[num_cols] = df[num_cols].fillna(0)
    df[str_cols] = df[str_cols].fillna("No Data")
    
    # Convert all columns to string
    df = df.astype(str)

    return df

def sanitize_index_name(index_name: str) -> str:
    """Replace invalid characters in Elasticsearch index name."""
    return re.sub(r'[\\/*?"<>|, ]', '_', index_name)

def bulk_index_to_es(es_client: Elasticsearch, df: pd.DataFrame, index_name: str) -> None:
    """Index dataframe to Elasticsearch with bulk method"""
    # Create index if not exists
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name)

    try:
        # Convert DataFrame to Elasticsearch bulk format
        actions = [
            {
                "_index": index_name,
                "_source": row.to_dict()
            }
            for _, row in df.iterrows()
        ]

        # Bulk index documents
        success, failed = bulk(es_client, actions)
        my_logger.info(f"Successfully indexed {success} documents.")
            
        if len(failed) > 0:
            my_logger.error(f"Failed to index {failed} documents.")

    except BadRequestError as e:
        my_logger.error(f"Elasticsearch Index Creation Failed: {e}")
    except Exception as e:
        my_logger.error(f"Something went wrong. {e}")
        

def read_and_push_to_es(es_client: Elasticsearch):
    """Check if CSV exists, download if missing, then process and index it."""
    try:
        csv_dir = "./csv_files"
        csv_filename = cfg.csv_file.name
        csv_file_path = os.path.join(csv_dir, csv_filename)
        
        if not os.path.exists(csv_file_path):
            my_logger.info("No CSV file found. Downloading from Dropbox...")
            dropbox_url = cfg.csv_file.dropbox_url
            download_file(dropbox_url, csv_file_path)
        df = pd.read_csv(csv_file_path)
        df = clean_dataframe(df)
        index_name = sanitize_index_name(csv_filename.split(".")[0])
        bulk_index_to_es(es_client, df, index_name)
        my_logger.info(f"Successfully read and pushed to {index_name}.")
    except Exception as e:
        my_logger.error(f"Read and push to Elasticsearch went wrong. {e}")

if __name__ == "__main__":
    es_client = Elasticsearch(cfg.elasticsearch.host, basic_auth=(cfg.elasticsearch.user, cfg.elasticsearch.password))
    read_and_push_to_es(es_client)    