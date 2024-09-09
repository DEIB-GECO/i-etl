import random

class Gene:
    def __init__(self):
        pass
    
    @staticmethod
    def generate_chromosome(*arguments):
        options = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y"]
        return random.choice(options)