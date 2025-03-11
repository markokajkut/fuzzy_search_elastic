import os
import streamlit as st
from elasticsearch import Elasticsearch
from logger import MyLogger
from fuzzy_worker import FuzzyWorker
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
    st.session_state['es_client'] = Elasticsearch(cfg.elasticsearch.host, basic_auth=(cfg.elasticsearch.user, cfg.elasticsearch.password))
    
# Initialize search filters if not already in session state
if "search_filters" not in st.session_state:
    st.session_state["search_filters"] = {}

# Function to reset search filters
def reset_search_filters(fields):
    """Reset fuzzy search filters stored as cached session state variables"""
    for field in fields:
        if field in st.session_state:
            del st.session_state[field]  # Remove field from session state
    st.session_state["search_filters"] = {}  # Reset dictionary

def main():
    """Main part of the streamlit app"""
    try:
        fuzzy_worker = FuzzyWorker(my_logger, st.session_state['es_client'])

        st.html("<div style='text-align: center; font-size: 32px; font-weight: bold;'>Fuzzy Search Elastic</div>")
        
        index_name = fuzzy_worker.get_index()
        if index_name:
            with st.sidebar:
            
                st.subheader("Index Name")
                st.markdown(f"***{index_name}***")
            
                if "df" not in st.session_state:
                    st.session_state["df"] = fuzzy_worker.get_data_from_es_index(index_name)
                                
                st.write("---")
                
                reset_filters = st.button("Reset Filters", type="primary")

            # Get the fields from Elasticsearch
            fields = fuzzy_worker.get_index_fields(index_name=index_name)

            # Ensure fields exist in session state before rendering widgets
            for field in fields:
                if field not in st.session_state["search_filters"]:
                    st.session_state["search_filters"][field] = ""


            if len(fields) > 0:
                # Create dynamic column layout with a max of 3 columns
                num_columns = min(3, len(fields))
                columns = st.columns(num_columns)

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
                st.rerun()

            search_filters = {field: value for field, value in st.session_state["search_filters"].items() if value.strip()}


            if not search_filters:  # If no filters applied, show full dataset
                df = st.session_state["df"]
            else:  # If filters are applied, perform search
                df = fuzzy_worker.multi_search_elasticsearch(index_name=index_name, queries=search_filters, fields=list(search_filters.keys()))

            st.dataframe(df)
    except Exception as e:
        st.error(e)
        
   
if __name__ == "__main__":
    main()     
