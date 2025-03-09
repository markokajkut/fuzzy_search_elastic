import pandas as pd
import argparse
import re
import os
from elasticsearch import Elasticsearch, BadRequestError
from elasticsearch.helpers import bulk
from logger import MyLogger
from dotenv import load_dotenv

load_dotenv()

my_logger = MyLogger().get_logger()


def clean_dataframe(df):
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

def sanitize_index_name(index_name):
    """Replace invalid characters in Elasticsearch index name."""
    return re.sub(r'[\\/*?"<>|, ]', '_', index_name)

def bulk_index_to_es(es_client, df, index_name):
    """Index dataframe to Elasticsearch with bulk method"""
    index_name = sanitize_index_name(index_name)
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
            return (f"Successfully indexed {success} documents.", f"Failed to index {failed} documents.")
        return f"Successfully indexed {success} documents."
    except BadRequestError as e:
        my_logger.error(f"Elasticsearch Index Creation Failed: {e}")
        return f"Elasticsearch Index Creation Failed: {e}"
    except Exception as e:
        my_logger.error(f"Something went wrong. {e}")
        return f"Something went wrong. {e}"


def read_and_push_to_es(es_client, csv_file_path):
    """Read CSV, clean it, and index to Elasticsearch"""
    try:
        df = pd.read_csv(csv_file_path)
        df = clean_dataframe(df)

        index_name = os.path.basename(csv_file_path).split(".")[0]

        return bulk_index_to_es(es_client, df, index_name)
        
    except Exception as e:
        my_logger.error(f"Read and push to Elasticsearch went wrong. {e}")
        return f"Read and push to Elasticsearch went wrong. {e}"

if __name__ == "__main__":
    es_client = Elasticsearch(os.getenv("ES_HOST_FROM_LOCAL"), basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")))
    
    parser = argparse.ArgumentParser(description="Ingest CSV file into Elasticsearch")
    parser.add_argument("csv_path", help="Path to the CSV file")
    
    args = parser.parse_args()
    result = read_and_push_to_es(es_client, args.csv_path)
    
    my_logger.info(result)
    