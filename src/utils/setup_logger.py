import logging
from time import strftime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s, %(levelname)-5s [%(module)s:%(funcName)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    handlers=[
        logging.FileHandler('log-{}.log'.format(strftime('%Y-%m-%d:%H:%M:%S'))),
        logging.StreamHandler()
    ]
)
log = logging.getLogger()
# do not allow PyMongo to print everything,
# only important messages (warning, error and fatal) wil be shown
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
