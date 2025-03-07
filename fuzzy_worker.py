import re
import pandas as pd
from elasticsearch import BadRequestError
from elasticsearch.helpers import bulk

class FuzzyWorker():
    def __init__(self, logger, es_client):
        self.logger = logger
        self.es_client = es_client
        # self.file_object = file_object
        # self.index_name = self.file_object.name.split(".")[0]
        # self.file_name = self.file_object.name.split(".")[0]
        # self.file_extension = self.file_object.name.split(".")[-1]
        # self.dataframe = pd.read_csv(self.file_object)

    @staticmethod
    def clean_dataframe(df):
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
    
    @staticmethod
    def sanitize_index_name(index_name):
        """Replace invalid characters in Elasticsearch index name."""
        return re.sub(r'[\\/*?"<>|, ]', '_', index_name)

    def index_to_es(self, df, index_name):
        # Create index if not exists
        if not self.es_client.indices.exists(index=index_name):
            self.es_client.indices.create(index=index_name)

        try:
            # Index each row
            for i, row in df.iterrows():
                doc = row.to_dict()
                response = self.es_client.index(index=self.sanitize_index_name(index_name), id=i, document=doc)
                self.logger.info(f"{response}")
        except Exception as e:
            self.logger.info(f"{response}")
            

    def bulk_index_to_es(self, df, index_name):
        
        index_name = self.sanitize_index_name(index_name)
        # Create index if not exists
        if not self.es_client.indices.exists(index=index_name):
            self.es_client.indices.create(index=index_name)

       # try:
        # Convert DataFrame to Elasticsearch bulk format
        actions = [
            {
                "_index": index_name,
                "_source": row.to_dict()
            }
            for _, row in df.iterrows()
        ]

        # Bulk index documents
        success, failed = bulk(self.es_client, actions)
        self.logger.info(f"Successfully indexed {success} documents.")
            
        #     if failed > 0:
        #         self.logger.error(f"Failed to index {failed} documents.")
        #         return (f"Successfully indexed {success} documents.", f"Failed to index {failed} documents.")
        #     return f"Successfully indexed {success} documents."
        # except BadRequestError as e:
        #     self.logger.error(f"Elasticsearch Index Creation Failed: {e}")
        #     return f"Elasticsearch Index Creation Failed: {e}"
        # except Exception as e:
        #     self.logger.error(f"Something went wrong. {e}")
        #     return f"Something went wrong. {e}"

    def read_and_push_to_es(self, file_object):
        try:
            df = pd.read_csv(file_object)
            df = self.clean_dataframe(df)
            self.bulk_index_to_es(df, file_object.name.split(".")[0])
            
        except Exception as e:
            self.logger.error(f"Read and push to Elasticsearch went wrong. {e}")
            return f"Read and push to Elasticsearch went wrong. {e}"
        
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
    
    def get_data_from_es_index(self, index_name):
        """Retrieve all documents from an Elasticsearch index and return a DataFrame."""
        if index_name:
            # Create index if not exists
            if self.es_client.indices.exists(index=index_name):
                
                scroll_time = "2m"  # Keep scroll context alive for 2 minutes
                batch_size = 1000  # Number of docs per batch

                # Initial search with scroll
                response = self.es_client.search(
                    index=index_name,
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
                # df = self.clean_dataframe(df)
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
    
    def multi_search_elasticsearch(self, index_name, queries, fields):
        """
        Searches Elasticsearch with multiple queries over multiple fields.

        :param queries: List of search terms or a dictionary of {field: query}
        :param fields: List of fields to search in
        :return: Pandas DataFrame with search results
        """
        # If queries is a list, create should conditions for all fields
        if isinstance(queries, list):
            should_conditions = [
                {"multi_match": {"query": query, "fields": fields, "fuzziness": "2"}}
                for query in queries
            ]
        # If queries is a dictionary, match each field with its respective query
        elif isinstance(queries, dict):
            should_conditions = [
                {"match": {field: {"query": query, "fuzziness": "2"}}}
                for field, query in queries.items()
            ]
        else:
            raise ValueError("Queries must be a list of terms or a dictionary of {field: query}")

        # Build the search body with bool query
        search_body = {
            "query": {
                "bool": {
                    "should": should_conditions,
                    "minimum_should_match": 1  # At least one query must match
                }
            }
        }

        # Execute the search
        res = self.es_client.search(index=index_name, body=search_body)
        hits = res["hits"]["hits"]

        # Convert results to DataFrame
        df = pd.DataFrame([hit["_source"] for hit in hits])
        
        return df
    
    def get_all_indexes(self):
        try:
            indices = self.es_client.cat.indices(format="json")  # Fetch index metadata in JSON format
            return [index["index"] for index in indices]  # Extract index names
        except Exception as e:
            print(f"Error retrieving indexes: {e}")
            return []

    def get_index_fields(self, index_name):
        try:
            mapping = self.es_client.indices.get_mapping(index=index_name)  # Get index mapping
            properties = mapping[index_name]["mappings"]["properties"]  # Extract field properties
            return list(properties.keys())  # Return field names as a list
        except Exception as e:
            print(f"Error retrieving fields for index '{index_name}': {e}")
            return []
