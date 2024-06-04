WORKING_DIR=working-dir
FILES_DIR=$WORKING_DIR/files
METADATA_DIR=$WORKING_DIR/metadata

# create the working-dir only if it does not exist yet
mkdir -p "$WORKING_DIR"
# Empty the files dir if needed
if [ -d "$FILES_DIR" ]
then
  rm -Rf $FILES_DIR
  mkdir "$FILES_DIR"
fi
# create the metadata dir if it does not exist yet
mkdir -p "$METADATA_DIR"

# Write the properties.ini files, with the above folders
PROPERTIES_FILE=properties.ini
CURRENT_DIR=$(pwd)
echo "[FILES]" > ${PROPERTIES_FILE} # empty the file and write the string
echo "working_dir=$CURRENT_DIR/$WORKING_DIR" >> ${PROPERTIES_FILE}
echo "files_dir=$CURRENT_DIR/$FILES_DIR" >> ${PROPERTIES_FILE}
echo "metadata_dir=$CURRENT_DIR/$METADATA_DIR" >> ${PROPERTIES_FILE}
echo "" >> ${PROPERTIES_FILE} # new line
echo "[DATABASE]" >> ${PROPERTIES_FILE}
echo "connection_string=mongodb://localhost:27017/" >> ${PROPERTIES_FILE}
echo "database_name=better_database_subset" >> ${PROPERTIES_FILE}

# Create the local Python environment
LOCAL_PYTHON_ENV=.venv-better-fairificator
python3 -m venv ${LOCAL_PYTHON_ENV}
source ${LOCAL_PYTHON_ENV}/bin/activate
python3 -m pip install --upgrade pip
pip3 install -r requirements.txt
