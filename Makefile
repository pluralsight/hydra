# Makefile for building and running Docker containers

# Define variables
DOCKER_IMAGE_NAME = hydra-publish-test
DOCKERFILE = Dockerfile.new

build:
	mkdir ps-publish
	sbt clean compile
	sbt universal:packageBin
	unzip ingest/target/universal/*.zip -d ps-publish
	docker build -t $(DOCKER_IMAGE_NAME) -f $(DOCKERFILE) .

run:
	docker run -d -p 8088:8088 --env-file .env --name ${DOCKER_IMAGE_NAME} ${DOCKER_IMAGE_NAME}

stop:
	docker stop $(DOCKER_IMAGE_NAME)
	docker rm $(DOCKER_IMAGE_NAME)

clean:
	docker stop $(DOCKER_IMAGE_NAME) || true
	docker rm $(DOCKER_IMAGE_NAME) || true
	docker rmi $(DOCKER_IMAGE_NAME) || true
	rm -rf ps-publish

help:
	@echo "Available targets:"
	@echo "  build   - Compile and package the code. Prep and Build the Docker image"
	@echo "  run     - Run the Docker container"
	@echo "  stop    - Stop and remove the Docker container"
	@echo "  clean   - Clean up all containers and images and delete ps-publish directory"
	@echo "  help    - Show this help message"


# By default, show the help message
.DEFAULT_GOAL := help
