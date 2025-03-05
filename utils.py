import pandas as pd
from elasticsearch import Elasticsearch

def clean_dataframe(df):
    # Replace "__NA__" and "N/A" with NaN first to standardize missing values
    df.replace(["__NA__", "N/A"], pd.NA, inplace=True)

    # Separate numerical and string columns
    num_cols = df.select_dtypes(include=['number']).columns
    str_cols = df.select_dtypes(include=['object', 'string']).columns

    # Fill NaNs accordingly
    df[num_cols] = df[num_cols].fillna(0)
    df[str_cols] = df[str_cols].fillna("No Data")

    return df

def index_to_es(df, es_client, index_name):
    # Create index if not exists
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name)

    # Index each row
    for i, row in df.iterrows():
        doc = row.to_dict()
        es_client.index(index=index_name, id=i, document=doc)
        
    return index_name


def read_and_push_to_es(es_client, file_object, file_name, file_extension):
    try:
        if file_extension == "csv":
            df = pd.read_csv(file_object)
            df = clean_dataframe(df)
            index_to_es(df, es_client, file_name)
        elif file_extension == "xlsx":
            dfs = pd.read_excel(file_object, sheet_name=None)  # Load all sheets
            for sheet in dfs.keys():
                dfs[sheet] = clean_dataframe(dfs[sheet])
                index_name = index_to_es(dfs[sheet], es_client, f"{file_name}_{sheet}")
        return "success", index_name
    except:
        return "failed", index_name
    
def get_data_from_es_index(es_client, index_name):
    # Query to fetch all documents
    query = {
        "query": {
            "match_all": {}  # Retrieves all documents
        },
        "size": 10000
    }

    # Retrieve documents
    response = es_client.search(index=index_name, body=query)  # Adjust size as needed

    # Extract the source data
    data = [doc["_source"] for doc in response["hits"]["hits"]]

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df
