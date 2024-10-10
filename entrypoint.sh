#!/bin/bash

if [ "${CONTEXT_MODE}" == "DEV" ]; then
  echo "Running main code"
  python3 src/main-etl.py
elif [ "${CONTEXT_MODE}" == "CATALOGUE" ]; then
  echo "Running catalogue"
  python3 src/main-catalogue.py
elif [ "${CONTEXT_MODE}" == "TEST" ]; then
  echo "Running tests"
  pytest tests/ --log-cli-level=DEBUG
else
  echo "Unrecognised context mode '${CONTEXT_MODE}'"
fi