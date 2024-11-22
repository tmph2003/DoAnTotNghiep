from datetime import datetime, date

DEFAULT_DATE_FORMAT = '%Y%m%d'
FULL_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
GMT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
SECONDS_IN_DAY = 60 * 60 * 24
SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_MINUTE = 60

DATE_ID_FORMAT = 'yyyyMMdd'
DEFAULT_INTEGER_VALUE = -999
DEFAULT_FLOAT_VALUE = -999.0
DEFAULT_STRING_VALUE = 'Unknown'
DEFAULT_BOOLEAN_VALUE = False
DEFAULT_DATE_ID_VALUE = 10101010
DEFAULT_TIMESTAMP_VALUE = datetime(1000, 10, 10)

MIN_CREATED_DATE = datetime.strptime('2021-08-01 00:00:00', DEFAULT_DATETIME_FORMAT)


def split_array(data: list, num_element_in_part=100):
    spited_data = []
    tmp_list = []
    for i in range(len(data)):
        if i != 0 and ((i % num_element_in_part) == 0 or i == len(data) - 1):
            tmp_list.append(data[i])
            spited_data.append(tmp_list)
            tmp_list = []
        else:
            tmp_list.append(data[i])
    return spited_data


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def to_bool(value):
    return 1 if value is not None and (value in ['true', True, 'True'] or to_float(value, default=0) >= 1) else 0


def is_null(value):
    return True if value is None or value in ['null', 'NULL', ''] else False


def trim_and_uppercase(value):
    if is_null(value):
        return None
    return value.strip().upper()


def to_int(value, default=None):
    try:
        return int(value or None)
    except:
        return default


def to_float(value, default=None):
    try:
        return float(value)
    except:
        return default


def to_dict(model, ignore=None):
    return {
        col.name: None if ignore is not None and col.name in ignore else getattr(model, col.name)
        for col in model.__table__.columns
    }


def convert_price(price):
    return None if price is None else price / 100000


def to_date_id(value):
    if value is None:
        value = datetime.now()
    return int(datetime.strftime(value, DEFAULT_DATE_FORMAT))


def convert_time(string_time):
    timestamp = datetime.fromtimestamp(string_time)
    return timestamp.strftime(DEFAULT_DATETIME_FORMAT)
