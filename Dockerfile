FROM python:3.12

# 1. install Python requirements
# do it first to avoid doing it every time the image is built with newer code
COPY requirements.txt /home/fairificator-deployed/requirements.txt
# install Python dependencies
# no venv needed because this runs within the Docker, thus without breaking the current OS
RUN pip install -r /home/fairificator-deployed/requirements.txt --no-cache-dir
# install locales in the Docker container
RUN apt-get update
RUN apt-get install -y locales locales-all


# 2. copy the ETL code into Docker instance
# The .env should NOT be copied because it should be
COPY src /home/fairificator-deployed/src
COPY README.md /home/fairificator-deployed/README.md
COPY entrypoint.sh /home/fairificator-deployed/entrypoint.sh
#COPY .env /home/fairificator-deployed/.env


# 3. for tests only, we copy tests too
COPY pytest.ini /home/fairificator-deployed/pytest.ini
COPY tests /home/fairificator-deployed/tests

COPY datasets/test /home/fairificator-deployed/test


# 4. script to be executed when RUNNING the Docker image
# the entrypoint script runs the ETL or the tests
# depending on the env. variable CONTEXT_MODE (DEV or TEST - default to DEV)
RUN chmod +x /home/fairificator-deployed/entrypoint.sh
ENTRYPOINT ["/home/fairificator-deployed/entrypoint.sh"]