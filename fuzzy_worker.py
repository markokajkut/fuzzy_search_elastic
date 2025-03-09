import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class FuzzyWorker():
    def __init__(self, logger, es_client):
        self.logger = logger
        self.es_client = es_client


    def index_to_es(self, df, index_name):
        """Index dataframe to Elasticsearch"""
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
        """Perform a fuzzy search to one field in Elasticsearch index, with fuzziness level 2"""
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
        """Perform a fuzzy search to more fields in Elasticsearch index, with fuzziness level 2"""
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
        """Get all indexes from Elasticsearch instance"""
        try:
            indices = self.es_client.cat.indices(format="json")  # Fetch index metadata in JSON format
            return [index["index"] for index in indices]  # Extract index names
        except Exception as e:
            print(f"Error retrieving indexes: {e}")
            return []

    def get_index_fields(self, index_name):
        """Get all fields from one Elasticsearch index"""
        try:
            mapping = self.es_client.indices.get_mapping(index=index_name)  # Get index mapping
            properties = mapping[index_name]["mappings"]["properties"]  # Extract field properties
            return list(properties.keys())  # Return field names as a list
        except Exception as e:
            self.logger(f"Error retrieving fields for index '{index_name}': {e}")
            return []
