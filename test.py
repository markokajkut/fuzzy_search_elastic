import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
import json

# Elasticsearch connection
es = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "changeme"))

# Constants
INDEX_NAME = "csv_data"

# Function to index CSV data
def index_csv(file):
    df = pd.read_csv(file)
    df = df.fillna("")  # Replace NaN values with empty strings

    # Create index if not exists
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME)

    # Index each row
    for i, row in df.iterrows():
        doc = row.to_dict()
        es.index(index=INDEX_NAME, id=i, document=doc)

    return df

# Function to search using Elasticsearch fuzzy match
def search_elasticsearch(query):
    search_body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["*"],  # Searches in all fields
                "fuzziness": "AUTO"
            }
        }
    }
    res = es.search(index=INDEX_NAME, body=search_body)
    hits = res["hits"]["hits"]
    return [hit["_source"] for hit in hits]

# Streamlit UI
st.title("CSV Browser with Fuzzy Search (Elasticsearch)")

# File upload
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
if uploaded_file:
    df = index_csv(uploaded_file)
    st.write("### Sample Data", df.head())

# Search bar
search_query = st.text_input("Search the CSV file:")
if search_query:
    results = search_elasticsearch(search_query)
    if results:
        st.write(pd.DataFrame(results))
    else:
        st.write("No matches found.")
