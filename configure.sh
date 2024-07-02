# Create the local Python environment
LOCAL_PYTHON_ENV=.venv-better-fairificator
python3 -m venv ${LOCAL_PYTHON_ENV}
source ${LOCAL_PYTHON_ENV}/bin/activate
python3 -m pip install --upgrade pip
pip3 install -r requirements.txt --no-cache-dir
