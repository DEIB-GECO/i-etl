#!/bin/bash

if [ "${CONTEXT_MODE}" == "DEV" ]; then
  echo "Running main code with env file ${ETL_ENV_FILE_NAME}"
  python3 src/main.py
elif [ "${CONTEXT_MODE}" == "TEST" ]; then
  echo "Running tests with env file ${ETL_ENV_FILE_NAME}"
  pytest tests/ --log-cli-level=DEBUG
else
  echo "Unrecognised context mode '${CONTEXT_MODE}'. Environment filename is ${ETL_ENV_FILE_NAME}."
fi