import os
import streamlit as st
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from logger import MyLogger
from fuzzy_worker import FuzzyWorker

load_dotenv()

my_logger = MyLogger().get_logger()

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

if 'es_client' not in st.session_state:
    st.session_state['es_client'] = Elasticsearch(os.getenv("ES_HOST"), basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")))
    
# Initialize search filters if not already in session state
if "search_filters" not in st.session_state:
    st.session_state["search_filters"] = {}

# Function to reset search filters
def reset_search_filters(fields):
    for field in fields:
        if field in st.session_state:
            del st.session_state[field]  # Remove field from session state
    st.session_state["search_filters"] = {}  # Reset dictionary


def main():

    st.html("<div style='text-align: center; font-size: 32px; font-weight: bold;'>Search for Keywords</div>")

    fuzzy_worker = FuzzyWorker(my_logger, st.session_state['es_client'])

    with st.sidebar:
        
        index_selection = st.selectbox(
        "Select Elasticsearch index to visualize",
        fuzzy_worker.get_all_indexes(),
        )
        
        st.write("---")
        
        with st.form("my-form", clear_on_submit=True):
            uploaded_file = st.file_uploader(
                "Choose a CSV File to push to Elasticsearch",
                type=["csv"],
                help="Load a CSV File to Search Over", 
            )
            submitted = st.form_submit_button("Upload file.")
        
        st.write("---")
        reset_filters = st.button("Reset Filters", type="primary")

    if submitted and uploaded_file is not None:
        resp = fuzzy_worker.read_and_push_to_es(uploaded_file)
        st.rerun()
        if resp:
            st.sidebar.error(resp)
        
    # Get the fields from Elasticsearch
    fields = fuzzy_worker.get_index_fields(index_name=index_selection)

    # Ensure fields exist in session state before rendering widgets
    for field in fields:
        if field not in st.session_state["search_filters"]:
            st.session_state["search_filters"][field] = ""


    if len(fields) > 0:
        # Create dynamic column layout with a max of 3 columns
        num_columns = min(3, len(fields))  # Max 3 columns
        columns = st.columns(num_columns)  # Create columns dynamically

    # Loop through fields and distribute across columns
    for i, field in enumerate(fields):
        col = columns[i % num_columns]
        with col:
            st.session_state["search_filters"][field] = st.text_input(
                field, 
                value=st.session_state.get(field, ""), 
                key=field
            )

    if reset_filters:
        reset_search_filters(fields)
        st.rerun()  # Force UI refresh

    search_filters = {field: value for field, value in st.session_state["search_filters"].items() if value.strip()}

    # Debug: Show the search filters dictionary
    # st.write(st.session_state["search_filters"])

    if not search_filters:  # If no filters applied, show full dataset
        df = fuzzy_worker.get_data_from_es_index(index_selection)
    else:  # If filters are applied, perform search
        df = fuzzy_worker.multi_search_elasticsearch(index_name=index_selection, queries=search_filters, fields=list(search_filters.keys()))

    if index_selection:
        #st.write(df.dtypes)
        st.dataframe(df)
        
   
if __name__ == "__main__":
    main()     
