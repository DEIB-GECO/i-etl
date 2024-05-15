import matplotlib
matplotlib.rcParams['backend'] = 'TkAgg'  # fix to avoid "Loaded backend macosx version unknown." error
import matplotlib.pyplot as plt
from pymongo.cursor import Cursor

from src.utils.setup_logger import log


class DistributionPlot:
    def __init__(self, cursor: Cursor, examination_name: str):
        self.cursor = cursor
        self.examination_name = examination_name
        self.__compute_x_and_y_from_cursor("_id", "total")

    def draw(self):
        log.debug(self.x)
        log.debug(self.y)
        plt.bar(self.x, self.y)
        plt.suptitle('Value distribution for Examination ' + self.examination_name)
        plt.show()

    def __compute_x_and_y_from_cursor(self, x_axis: str, y_axis: str):
        log.debug(self.cursor)
        self.x = []
        self.y = []
        for element in self.cursor:
            log.debug(element)
            self.x.append(str(element[x_axis]))
            self.y.append(element[y_axis])
