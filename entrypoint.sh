#!/bin/bash

ls -a /home/fairificator-deployed/test/

if [ "${CONTEXT_MODE}" == "DEV" ]; then
  echo "Running main code with env file ${ETL_ENV_FILE_NAME}"
  python3 src/main-etl.py
elif [ "${CONTEXT_MODE}" == "GENERATION" ]; then
  echo "Generating data with env file ${ETL_ENV_FILE_NAME}"
  python3 src/main-generation.py
elif [ "${CONTEXT_MODE}" == "TEST" ]; then
  echo "Running tests with env file ${ETL_ENV_FILE_NAME}"
  pytest tests/ --log-cli-level=DEBUG
else
  echo "Unrecognised context mode '${ETL_ENV_FILE_NAME}'"
fi