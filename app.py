import os
import streamlit as st
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from logger import MyLogger
from fuzzy_worker import FuzzyWorker

load_dotenv()

my_logger = MyLogger()

st.set_page_config(
    page_title="Fuzzy Search",
    page_icon=":material/search:",
    layout="wide",
    
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None
    }
)

st.html("<div style='text-align: center; font-size: 32px; font-weight: bold;'>Search for Keywords</div>")
es_client = Elasticsearch(os.getenv("ES_HOST"), basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")))
index_name = None

with st.sidebar:
    uploaded_file = st.file_uploader(
        "Choose a CSV File",
        type=["csv"],
        help="Load a CSV File to Search Over", 
    )
    
if uploaded_file is not None:
    fuzzy_worker = FuzzyWorker(my_logger, es_client, uploaded_file)
    fuzzy_worker.clean_dataframe()
    st.dataframe(fuzzy_worker.dataframe)

search_filters = {}
with st.sidebar:
    for column in fuzzy_worker.dataframe.columns:
        search_filters[column] = st.text_input(column)
        fuzzy_worker.dataframe = fuzzy_worker.search_elasticsearch(search_filters[column], column)
st.info(search_filters)
    
    
        
        
