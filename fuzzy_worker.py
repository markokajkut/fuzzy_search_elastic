import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class FuzzyWorker():
    def __init__(self, logger, es_client, file_object):
        self.logger = logger
        self.es_client = es_client
        self.file_object = file_object
        self.index_name = self.file_object.name.split(".")[0]
        self.file_name = self.file_object.name.split(".")[0]
        self.file_extension = self.file_object.name.split(".")[-1]
        self.dataframe = pd.read_csv(self.file_object)

    def clean_dataframe(self):
        # Replace "__NA__" and "N/A" with NaN first to standardize missing values
        self.dataframe.replace(["__NA__", "N/A"], pd.NA, inplace=True)

        # Separate numerical and string columns
        num_cols = self.dataframe.select_dtypes(include=['number']).columns
        str_cols = self.dataframe.select_dtypes(include=['object', 'string']).columns

        # Fill NaNs accordingly
        self.dataframe[num_cols] = self.dataframe[num_cols].fillna(0)
        self.dataframe[str_cols] = self.dataframe[str_cols].fillna("No Data")

        return self.dataframe

    def index_to_es(self):
        # Create index if not exists
        if not self.es_client.indices.exists(index=self.index_name):
            self.es_client.indices.create(index=self.index_name)

        try:
            # Index each row
            for i, row in self.dataframe.iterrows():
                doc = row.to_dict()
                response = self.es_client.index(index=self.index_name, id=i, document=doc)
                self.logger.info(f"{response}")
        except Exception as e:
            self.logger.info(f"{response}")
            

    def bulk_index_to_es(self):

        # Convert DataFrame to Elasticsearch bulk format
        actions = [
            {
                "_index": self.index_name,
                "_source": row.to_dict()
            }
            for _, row in self.dataframe.iterrows()
        ]

        # Bulk index documents
        success, failed = bulk(self.es_client, actions)
        self.logger.info(f"Successfully indexed {success} documents.")
        if failed > 0:
            self.logger.error(f"Failed to index {failed} documents.")


    def read_and_push_to_es(self):
        try:
            # if self.file_extension == "csv":
            self.clean_dataframe()
            self.bulk_index_to_es()
                
            # elif self.file_extension == "xlsx":
            #     self.xlsx_sheets = pd.read_excel(self.file_object, sheet_name=None)  # Load all sheets
            #     for sheet in self.xlsx_sheets.keys():
            #         self.xlsx_sheets[sheet] = self.clean_dataframe(self.dataframe[sheet])
            #         self.bulk_index_to_es(dfs[sheet], es_client, f"{file_name}_{sheet}")
        except Exception as e:
            self.logger.error(f"Read and push to Elasticsearch went wrong. {e}")
        
    # def get_data_from_es_index(self):
    #     # Query to fetch all documents
    #     query = {
    #         "query": {
    #             "match_all": {}  # Retrieves all documents
    #         },
    #         "size": 10000
    #     }

    #     # Retrieve documents
    #     response = self.es_client.search(index=self.index_name, body=query)  # Adjust size as needed

    #     # Extract the source data
    #     data = [doc["_source"] for doc in response["hits"]["hits"]]

    #     # Convert to DataFrame
    #     df = pd.DataFrame(data)
    #     return df
    
    def get_data_from_es_index(self):
        """Retrieve all documents from an Elasticsearch index and return a DataFrame."""
        scroll_time = "2m"  # Keep scroll context alive for 2 minutes
        batch_size = 1000  # Number of docs per batch

        # Initial search with scroll
        response = self.es_client.search(
            index=self.index_name,
            body={"query": {"match_all": {}}},
            scroll=scroll_time,
            size=batch_size
        )

        # Extract initial batch
        data = [doc["_source"] for doc in response["hits"]["hits"]]
        scroll_id = response["_scroll_id"]

        while response["hits"]["hits"]:
            # Fetch next batch
            response = self.es_client.scroll(scroll_id=scroll_id, scroll=scroll_time)
            scroll_id = response["_scroll_id"]  # Update scroll ID
            data.extend([doc["_source"] for doc in response["hits"]["hits"]])  # Append new results

        # Clear scroll context
        self.es_client.clear_scroll(scroll_id=scroll_id)

        # Convert to DataFrame
        df = pd.DataFrame(data)
        return df
    
    def search_elasticsearch(self, query, column):
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [f"{column}"],
                    "fuzziness": "2"
                }
            }
        }
        
        res = self.es_client.search(index=self.index_name, body=search_body)
        hits = res["hits"]["hits"]

        # Convert results to DataFrame
        df = pd.DataFrame([hit["_source"] for hit in hits])

        return df

