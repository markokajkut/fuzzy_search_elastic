# Elasticsearch Fuzzy Search Streamlit App

This project provides a **Streamlit-based web application** for performing fuzzy searches on data stored in an **Elasticsearch** index. The application runs in a **Docker Compose** environment, with two services:

1. **Elasticsearch** - A search engine for indexing and querying data.
2. **Streamlit App** - A user interface for performing fuzzy searches on indexed data.

## Setup & Running the Application

### **1. Run the Application Using Docker Compose**

You can start the application using Docker Compose:

```sh
docker compose up
```

Or run it in the background (detached mode):

```sh
docker compose up -d
```

This will start **Elasticsearch** and the **Streamlit app**.

### **2. Ingest CSV Data into Elasticsearch**

To index a CSV file into Elasticsearch, run the following command from your local machine:

```sh
python add_csv.py /path/to/csv/file
```

**Note:** The ingestion script must be executed locally (outside Docker) to connect to Elasticsearch properly. Streamlit app checks for new indexes every 30s.

## Environment Variables (`.env`)

The project relies on the following environment variables:

| Variable             | Description                                                                     |
| -------------------- | ------------------------------------------------------------------------------- |
| `ES_HOST`            | URL of Elasticsearch inside Docker (`http://elasticsearch:9200`)                |
| `ES_USER`            | Elasticsearch username (default `elastic`)                                      |
| `ES_PASS`            | Elasticsearch password (default `changeme`)                                     |
| `ES_HOST_FROM_LOCAL` | URL of Elasticsearch when accessed from local machine (`http://localhost:9200`) |

Ensure you set up a `.env` file with the correct values before running the application.

## Useful Commands

- **Stop the application:**
  ```sh
  docker compose down
  ```
- **Check running containers:**
  ```sh
  docker ps
  ```
- **View logs for the app:**
  ```sh
  docker logs -f <container_name>
  ```
- **Rebuild the application (if changes were made):**
  ```sh
  docker compose up --build
  ```

## Features

- Streamlit UI for searching indexed data
- Fuzzy search capabilities
- Elasticsearch integration
- CSV data ingestion via script

## Troubleshooting

- If the app cannot connect to Elasticsearch, ensure that the service is running:
  ```sh
  docker ps
  ```
- If you get an **authentication error**, check the `ES_USER` and `ES_PASS` values.
- If indexing fails, verify that the CSV file is correctly formatted.
