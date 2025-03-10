# Clean up dangling Docker images, containers, and caches
docker-clean:
	docker image prune -f
	docker container prune -f
	docker system prune -f

# Build Elasticsearch and Streamlit with Docker Compose
docker-up:
	docker compose up -d
