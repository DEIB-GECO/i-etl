import os

WORKING_DIR = "working-dir"
DEFAULT_DB_NAME = "better_default"
# these constants have to exactly match the volume paths described in compose.yaml
DOCKER_FOLDER = "/home/better-deployed"
DOCKER_FOLDER_DATA = os.path.join(DOCKER_FOLDER, "hospital-data")
DOCKER_FOLDER_GENERATED_DATA = os.path.join(DOCKER_FOLDER_DATA, "gen-data")
DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER_DATA, "metadata")
DOCKER_FOLDER_PHENOTYPIC = os.path.join(DOCKER_FOLDER_DATA, "phenotypic")
DOCKER_FOLDER_SAMPLE = os.path.join(DOCKER_FOLDER_DATA, "sample")
DOCKER_FOLDER_DIAGNOSIS = os.path.join(DOCKER_FOLDER_DATA, "diagnosis")
DOCKER_FOLDER_MEDICINE = os.path.join(DOCKER_FOLDER_DATA, "medicine")
DOCKER_FOLDER_IMAGING = os.path.join(DOCKER_FOLDER_DATA, "imaging")
DOCKER_FOLDER_GENOMIC = os.path.join(DOCKER_FOLDER_DATA, "genomic")
DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER_DATA, "pids")
DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER_DATA, "test")
DOCKER_FOLDER_PREPROCESSED = "preprocessed"  # where the preprocessed files go before being used by the ETL
DB_CONNECTION = "mongodb://mongo:27017/"

# the folder containing ground data for data generation
GROUND_DATA_FOLDER_FOR_GENERATION = os.path.join(DOCKER_FOLDER, "src", "generators", "data")
GROUND_METADATA_FOLDER_FOR_GENERATION = os.path.join(DOCKER_FOLDER, "src", "generators", "metadata")

# # to work locally without Docker
# DOCKER_FOLDER_DATA = "/Users/nelly/better-deployed/hospital-data"
# DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER_DATA, "metadata")
# DOCKER_FOLDER_PHENOTYPIC = os.path.join(DOCKER_FOLDER_DATA, "phenotypic")
# DOCKER_FOLDER_DIAGNOSIS = os.path.join(DOCKER_FOLDER_DATA, "diagnosis")
# DOCKER_FOLDER_MEDICINE = os.path.join(DOCKER_FOLDER_DATA, "medicine")
# DOCKER_FOLDER_IMAGING = os.path.join(DOCKER_FOLDER_DATA, "imaging")
# DOCKER_FOLDER_GENOMIC = os.path.join(DOCKER_FOLDER_DATA, "genomic")
# DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER_DATA, "pids")
# DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER_DATA, "test")
# DB_CONNECTION = "mongodb://localhost:27017/"

TEST_DB_NAME = "better_test"
