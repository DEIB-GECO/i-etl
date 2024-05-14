from database.Database import *
from etl.ETL import *
from utils.setup_logger import log
import sys


if __name__ == '__main__':

    log.info("Hello world!")

    argList = sys.argv
    log.info("Argument list is: " + str(argList))

    if len(argList) != 4:
        raise Exception(
            "This script expects 3 arguments: hospital name, file path to variable description, and file path to "
            "samples. Please make sure that you provided them correctly.")

    # argList[0] is the script filename, i.e., main.py
    hospitalName = argList[1]
    variablesFilepath = argList[2]
    samplesFilepath = argList[3]

    # variablesFilepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/data/metadata/IT-Buzzi-variables.csv"
    # samplesFilepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/data/samples/BUZZI/buzzi_subset_small_quoted.csv"
    etl = ETL(hospitalName, variablesFilepath, samplesFilepath)
    etl.run()

    log.info("Goodbye world!")
