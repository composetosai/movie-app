# Movie Data Store Restful API
This project aims to build a store of movie data accessible via a Restful API. The data is sourced from multiple data providers and is expected to use the same data format (CSV). Updates from data providers are sent via AWS S3, and each update event corresponds to a single movie. The API allows querying of data by year, movie name, cast member, or genre.

# Implementation Details
* **Programming Language**: Python
* **Database**: SQLite
* **Containerization**: Docker is used for containerizing the application.
* **Orchestration**: Kubernetes is used to deploy and manage the API workload in a cluster.
* **Kubernetes Setup**: Minikube is used for local Kubernetes setup.
* **Image Repository**: DockerHub is used for hosting Docker images.

## Setup Instructions
To set up the API locally or in a Kubernetes cluster, follow these steps:

## 1.Clone the Repository:

`git clone https://github.com/composetosai/movie-app.git`

## 2. Build Docker Image:

`docker build -t composetosai/python-app:latest .`

## 3. Run Docker Container:

`docker run -d -p 5000:5000 composetosai/python-app:latest`

## 4. Deploy to Kubernetes:

Apply the Kubernetes manifest file (movie-api-deployment.yaml) to deploy the API to a Kubernetes cluster.

`kubectl apply -f movie-api-deployment.yaml`

## 5. Access the API:

Once deployed, you can access the API endpoints via <api_url>.

## Data Format
The data format used for movie information is in CSV format. Data providers send updates via AWS S3, and the API is responsible for processing and storing the data perpetually.
