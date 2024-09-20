import os

WORKING_DIR = "working-dir"
DEFAULT_DB_NAME = "better_default"
# these constants have to exactly match the volume paths described in compose.yaml
DOCKER_FOLDER_DATA = "/home/better-deployed/hospital-data"
DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER_DATA, "metadata")
SERVER_FOLDER_DIAGNOSIS_REGEXES = os.path.join(DOCKER_FOLDER_DATA, "regexes")
DOCKER_FOLDER_LABORATORY = os.path.join(DOCKER_FOLDER_DATA, "laboratory")
DOCKER_FOLDER_DIAGNOSIS = os.path.join(DOCKER_FOLDER_DATA, "diagnosis")
DOCKER_FOLDER_MEDICINE = os.path.join(DOCKER_FOLDER_DATA, "medicine")
DOCKER_FOLDER_IMAGING = os.path.join(DOCKER_FOLDER_DATA, "imaging")
DOCKER_FOLDER_GENOMIC = os.path.join(DOCKER_FOLDER_DATA, "genomic")
DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER_DATA, "pids")
DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER_DATA, "test")
DB_CONNECTION = "mongodb://mongo:27017/"

# # to work locally without Docker
# DOCKER_FOLDER_DATA = "/Users/nelly/better-deployed/hospital-data"
# DOCKER_FOLDER_METADATA = os.path.join(DOCKER_FOLDER_DATA, "metadata")
# SERVER_FOLDER_DIAGNOSIS_REGEXES = os.path.join(DOCKER_FOLDER_DATA, "regexes")
# DOCKER_FOLDER_LABORATORY = os.path.join(DOCKER_FOLDER_DATA, "laboratory")
# DOCKER_FOLDER_DIAGNOSIS = os.path.join(DOCKER_FOLDER_DATA, "diagnosis")
# DOCKER_FOLDER_MEDICINE = os.path.join(DOCKER_FOLDER_DATA, "medicine")
# DOCKER_FOLDER_IMAGING = os.path.join(DOCKER_FOLDER_DATA, "imaging")
# DOCKER_FOLDER_GENOMIC = os.path.join(DOCKER_FOLDER_DATA, "genomic")
# DOCKER_FOLDER_ANONYMIZED_PATIENT_IDS = os.path.join(DOCKER_FOLDER_DATA, "pids")
# DOCKER_FOLDER_TEST = os.path.join(DOCKER_FOLDER_DATA, "test")
# DB_CONNECTION = "mongodb://localhost:27017/"

TEST_DB_NAME = "better_test"
