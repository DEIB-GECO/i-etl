import logging

import headfake.fieldset
import urllib3

logging.basicConfig(
    level=logging.DEBUG,
    # format='%(asctime)s, %(levelname)-5s [%(module)s:%(funcName)s:%(lineno)d] %(message)s',
    format='[%(module)s:%(funcName)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    handlers=[
        logging.StreamHandler()
        # we will add a FileHandler in the Execution class when we know the db name (user input)
    ]
)
log = logging.getLogger()
# do not allow PyMongo to print everything,
# only important messages (warning, error and fatal) wil be shown
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("faker").setLevel(logging.ERROR)
logging.getLogger("factory").setLevel(logging.ERROR)
logging.getLogger("headfake").setLevel(logging.ERROR)
logging.getLogger("faker").setLevel(logging.ERROR)
logging.getLogger("fieldset").setLevel(logging.ERROR)

