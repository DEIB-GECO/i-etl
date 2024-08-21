FROM python:3.12

# 1. install Python requirements
# do it first to avoid doing it every time the image is built with newer code
COPY requirements.txt /home/better-deployed/requirements.txt
# install Python dependencies
# no venv needed because this runs within the Docker, thus without breaking the current OS
RUN pip install -r /home/better-deployed/requirements.txt --no-cache-dir
# install locales in the Docker container
RUN apt-get update
RUN apt-get install -y locales locales-all


# 2. copy the ETL code into Docker instance
# The .env should NOT be copied because it should be
COPY src /home/better-deployed/src
COPY README.md /home/better-deployed/README.md
COPY entrypoint.sh /home/better-deployed/entrypoint.sh
#COPY .env /home/better-deployed/.env


# 3. for tests only, we copy tests too
COPY pytest.ini /home/better-deployed/pytest.ini
COPY tests /home/better-deployed/tests
# no need to copy test data, we can access it through a shared folder (volume)
# COPY datasets/test /home/better-deployed/datasets/test


# 4. script to be executed when RUNNING the Docker image
# the entrypoint script runs the ETL or the tests
# depending on the env. variable CONTEXT_MODE (DEV or TEST - default to DEV)
RUN chmod +x /home/better-deployed/entrypoint.sh
ENTRYPOINT ["/home/better-deployed/entrypoint.sh"]