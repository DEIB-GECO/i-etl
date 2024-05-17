from dateutil.parser import parse
from datetime import datetime

if __name__ == '__main__':
    print("Hello world!")
    current_date = '31/01/22 23:59:59'
    current_format = '%d/%m/%y %H:%M:%S'
    current_result = datetime.strptime(current_date, current_format)
    print(current_result)

    new_format = '%Y-%m-%d %H:%M:%S'
    new_result = current_result.strftime(new_format)
    print(new_result)

    print(parse(current_date))

    print("Goodbye world!")
