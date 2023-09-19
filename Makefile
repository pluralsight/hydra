# Makefile for building and running Docker containers

# Define variables
DOCKER_IMAGE_NAME = hydra-publish-test
DOCKERFILE = Dockerfile.new
PORT_MAPPING = -p 8088:8088
ENV_FILE = .env

# Target to build the Docker image
build:
	mkdir ps-publish
	sbt clean compile
	sbt universal:packageBin
	unzip ingest/target/universal/*.zip -d ps-publish
	docker build -t $(DOCKER_IMAGE_NAME) -f $(DOCKERFILE) .

# Target to run the Docker container
run:
	docker run -d $(PORT_MAPPING) --env-file $(ENV_FILE) --name ${DOCKER_IMAGE_NAME} $(DOCKER_IMAGE_NAME)

# Target to stop and remove the Docker container
stop:
	docker stop $(DOCKER_IMAGE_NAME)
	docker rm $(DOCKER_IMAGE_NAME)

# Target to clean up all containers and images
clean:
	docker stop $(DOCKER_IMAGE_NAME) || true
	docker rm $(DOCKER_IMAGE_NAME) || true
	docker rmi $(DOCKER_IMAGE_NAME) || true
	rm -rf ps-publish

# Target to show available targets
help:
	@echo "Available targets:"
	@echo "  build   - Build the Docker image"
	@echo "  run     - Run the Docker container"
	@echo "  stop    - Stop and remove the Docker container"
	@echo "  clean   - Clean up all containers and images"
	@echo "  help    - Show this help message"

# By default, show the help message
.DEFAULT_GOAL := help
