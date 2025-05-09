import os

WORKING_DIR = "working-dir"
DEFAULT_DB_NAME = "better_default"
# these constants have to exactly match the volume paths described in compose.yaml
DOCKER_FOLDER = "/home/i-etl-deployed"
DOCKER_FOLDER_DATA = os.path.join(DOCKER_FOLDER, "real-data")
DOCKER_FOLDER_GENERATED_DATA = os.path.join(DOCKER_FOLDER, "synthetic-data")
DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER, "metadata")
DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER, "pids")
DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER, "datasets", "test")
DB_CONNECTION = "mongodb://mongo:27017/"

# the folder containing ground data for data generation
GROUND_DATA_FOLDER_FOR_GENERATION = os.path.join(DOCKER_FOLDER, "src", "generators", "data")
GROUND_METADATA_FOLDER_FOR_GENERATION = os.path.join(DOCKER_FOLDER, "src", "generators", "metadata")

TEST_DB_NAME = "better_test"
