CURRENT_DIR=$(pwd)
WORKING_DIR=working-dir

echo "The working directory is $CURRENT_DIR/$WORKING_DIR"

# create the working-dir only if it does not exist yet
mkdir -p "$WORKING_DIR"

# Write the properties.ini files, with the above folders
PROPERTIES_FILE=properties.ini
echo "[FILES]" > ${PROPERTIES_FILE} # empty the file and write the string
echo "working_dir=$CURRENT_DIR/$WORKING_DIR" >> ${PROPERTIES_FILE}
echo "" >> ${PROPERTIES_FILE} # new line
echo "[DATABASE]" >> ${PROPERTIES_FILE}
echo "connection=mongodb://localhost:27017/" >> ${PROPERTIES_FILE}
echo "name=better_database" >> ${PROPERTIES_FILE}
echo "reset=false" >> ${PROPERTIES_FILE}
echo "" >> ${PROPERTIES_FILE}
echo "[HOSPITAL]" >> ${PROPERTIES_FILE}
echo "name=IT_BUZZI_UC1" >> ${PROPERTIES_FILE}

# Create the local Python environment
LOCAL_PYTHON_ENV=.venv-better-fairificator
python3 -m venv ${LOCAL_PYTHON_ENV}
source ${LOCAL_PYTHON_ENV}/bin/activate
python3 -m pip install --upgrade pip
pip3 install -r requirements.txt --no-cache-dir
