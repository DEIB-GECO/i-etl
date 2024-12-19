#!/bin/bash

if [ "${CONTEXT_MODE}" == "DEV" ]; then
  echo "Running main code with env file ${MY_ENV_FILE}"
  python3 src/main-etl.py
elif [ "${CONTEXT_MODE}" == "GENERATION" ]; then
  echo "Generating data with env file ${MY_ENV_FILE}"
  python3 src/main-generation.py
elif [ "${CONTEXT_MODE}" == "TEST" ]; then
  echo "Running tests with env file ${MY_ENV_FILE}"
  pytest tests/test_Transform.py --log-cli-level=DEBUG
else
  echo "Unrecognised context mode '${CONTEXT_MODE}'"
fi