# BETTER-fairificator
The fairification tools for BETTER projet data. 


### Installation

**From the root of the project, i.e., in `BETTER-fairificator` foler**

1. Make sure to have a recent Python version, e.g., Python 3.12 (tested with Python 3.12 only)
2. Create your own (new) virtual environment, e.g., named `.venv-better-fhir`: `/usr/local/bin/python3.12 -m venv .venv-better-fhir`
3. Source your new virtual environment to make it active: `source .venv-better-fhir/bin/activate`
4. Verify that your virtual environment is active and has been created with the expected Python version (especially important if you have multiple Python versions): 
   - `which python` should output the virtual environment path
   - `<your/venv/path/bin/python` should output Pyhon 3.12 (or equivalent)
5. Install the dependencies with the requirement file: `pip3 install -r requirements.txt`
6. To run the ETL script: `.venv-better-fhir/bin/python3.12 -m src.main`

