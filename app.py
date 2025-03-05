import streamlit as st
from elasticsearch import Elasticsearch

from utils import read_and_push_to_es, get_data_from_es_index

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
es_client = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "changeme"))
index_name = None

with st.sidebar:
    uploaded_file = st.file_uploader(
        "Choose a CSV/Excel File",
        type=["csv", "xlsx"],
        help="Load a CSV/Excel File to Search Over", 
    )
    if uploaded_file:
        file_name, file_extension = uploaded_file.name.split(".")
        try:
            msg, index_name = read_and_push_to_es(es_client, uploaded_file, file_name, file_extension)
        except Exception as e:
            st.error("Something went wrong.")
        
df = get_data_from_es_index(es_client, index_name)
st.dataframe(df)
        
    
        
        
