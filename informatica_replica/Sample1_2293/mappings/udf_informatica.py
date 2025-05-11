# Databricks notebook source
# MAGIC %sql
# MAGIC -- ADD_TO_DATE UDF
# MAGIC
# MAGIC CREATE FUNCTION IF NOT EXISTS ADD_TO_DATE(date TIMESTAMP, format STRING, amount INTEGER) RETURNS TIMESTAMP CONTAINS SQL RETURN
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN upper(format) IN ('Y', 'YY', 'YYY', 'YYYY') THEN dateadd(YEAR, amount, date)
# MAGIC     WHEN upper(format) IN ('MM', 'MON', 'MONTH') THEN dateadd(MONTH, amount, date)
# MAGIC     WHEN upper(format) IN ('D', 'DD', 'DDD', 'DY', 'DAY') THEN dateadd(DAY, amount, date)
# MAGIC     WHEN upper(format) IN ('HH', 'HH12', 'HH24') THEN dateadd(HOUR, amount, date)
# MAGIC     WHEN upper(format) IN ('MI') THEN dateadd(MINUTE, amount, date)
# MAGIC     WHEN upper(format) IN ('SS') THEN dateadd(SECOND, amount, date)
# MAGIC     WHEN upper(format) IN ('MS') THEN dateadd(MILLISECOND, amount, date)
# MAGIC     WHEN upper(format) IN ('US') THEN dateadd(MICROSECOND, amount, date)
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION IF NOT EXISTS IS_SPACES(value STRING) RETURNS BOOLEAN CONTAINS SQL RETURN
# MAGIC 	SELECT IF(LENGTH(TRIM(value)) == 0, True, False)

# COMMAND ----------

# udf for IN function
def in_function(*input_str):
    search_value = input_str[0]
    list_values = input_str[1:-1]
    case_flag = input_str[-1]

    if search_value is None:
        result = None
    elif case_flag is None:
        result = search_value in list_values
        if(result == 0):
            return None     
    elif case_flag == 0:
        result = search_value.lower() in [val.lower() for val in list_values]
    elif case_flag == 1:
        result = search_value in list_values
    else:
        list_values = input_str[1:]
        result = search_value in list_values
    return result

spark.udf.register("IN",in_function)

# COMMAND ----------

# UDF for Trunc(Number) and Trunc(Date)
import datetime
from decimal import Decimal

def trunc(value, format_or_precision=None):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
            if format_or_precision is None:
                return value.strftime('00-00-00 00:00:00.000000')
            elif format_or_precision.lower() in ('yyyy', 'yyy', 'yy','y'):
                return value.strftime('%Y-01-01 00:00:00.000000')
            elif format_or_precision.lower() in ('mm', 'mon', 'month'):
                return value.strftime('%Y-%m-01 00:00:00.000000')
            elif format_or_precision.lower() in ('d', 'dd', 'ddd', 'dy','day'):
                return value.strftime('%Y-%m-%d 00:00:00.000000')
            elif format_or_precision.lower() in ('hh', 'hh12', 'hh24'):
                return value.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f')
            elif format_or_precision.lower() == 'mi':
                return value.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f')
            elif format_or_precision.lower() in ('ss', 'ms', 'us'):
                return value.replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                return value.strftime('%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            return None
            
    elif isinstance(value, (int, float, Decimal)):
        if format_or_precision is None:
            return float(int(value))
        elif isinstance(format_or_precision, int):
            precision = format_or_precision
            if precision > 0:
                factor = 10 ** precision
                return int(value * factor) / factor
            elif precision < 0:
                scale = 10**(-precision)
                return float(int(value / scale) * scale)
        else:
            return value
    else:
        return value
    
spark.udf.register("TRUNC", trunc)


# COMMAND ----------

# UDF for to_integer
def to_integer(value, flag=False):
    if value is None:
        return None
    try:
        value = float(value)

        if flag:
            result = int(value)
        else:
            result = round(value)

        return result
    except ValueError:
        return 0

spark.udf.register("TO_INTEGER",to_integer)


# COMMAND ----------

#UDF for TO_DECIMAL
def to_decimal(value, scale=None):
    if value is None or not str(value).strip():
        return None
    
    try:
        value = str(value)
        numeric_part = ""
        first_non_numeric = False

        for char in value:
            if char.isdigit() or (char == '.' and not first_non_numeric):
                numeric_part += char
            elif numeric_part:
                break
            else:
                if char != '-' and char != ' ':
                    first_non_numeric = True

        if not numeric_part or first_non_numeric:
            return 0
        
        if scale is None:
            number = float(numeric_part)
        else:
            number = round(float(numeric_part), scale)

        return number

    except ValueError as e:
        return 0

spark.udf.register("TO_DECIMAL", to_decimal)

# COMMAND ----------

#UDF for TO_BIGINT

from decimal import Decimal
def TO_BIGINT(value, flag=False):
    if value is None:
        return None
    value_str = str(value)
    if any(char.isalpha() for char in value_str):
        return 0
    try:
        if flag:
            return int(Decimal(value_str))
        else:
            return int(round(Decimal(value_str)))
    except ValueError as e:
        raise Exception("Error: " + str(e))

spark.udf.register("TO_BIGINT", TO_BIGINT)

# COMMAND ----------

# UDF for TO_CHAR
import datetime
import decimal

date_format_mapping = {
    'D': '%w',
    'DAY': '%A',
    'DD': '%d',
    'DDD': '%j',
    'HH': '%H',
    'HH12': '%I',
    'HH24': '%H',
    'MON DD YYYY': '%b %d %Y',
    'J': lambda date_obj: str((date_obj - datetime.datetime(1858, 11, 17)).days),
    'MM/DD/RR': '%m/%d/%y',
    'MM/DD/YY': '%m/%d/%y',
    'SSSSS': lambda date_obj: str(int((date_obj - datetime.datetime(date_obj.year, date_obj.month, date_obj.day)).total_seconds())),
    'W': lambda date_obj: f'{(date_obj.day - 1) // 7 + 1:02}',
    'WW': '%V'
}

def to_char(value, format_string=None):
    if value is None:
        return None

    if isinstance(value, (float, decimal.Decimal, int)):
        if isinstance(value, float):
            if -1e16 < value <= -1e-16 or 1e-16 <= value < 1e16:
                return format(value, ".15f").rstrip('0').rstrip('.')
            else:
                return format(value, ".15e")
        else:
            return str(value)

    if isinstance(value, str):
        if format_string is not None:
            format_handler = date_format_mapping.get(format_string)
            if format_handler:
                try:
                    date_obj = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    if callable(format_handler):
                        return format_handler(date_obj)
                    else:
                        return date_obj.strftime(format_handler)
                except ValueError:
                    pass

                try:
                    date_obj = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    if callable(format_handler):
                        return format_handler(date_obj)
                    else:
                        return date_obj.strftime(format_handler)
                except ValueError:
                    pass

        else:
            return None
    
    return None

spark.udf.register("TO_CHAR", to_char)


# COMMAND ----------

#UDF for REPLACECHR
import re

def replacechr(case_flag, input_string, old_char_set, new_char):
    case_sensitive = True if case_flag != 0 else False

    if input_string is None:
        return None

    if old_char_set is None or old_char_set == '':
        return input_string

    if old_char_set == "CHR(39)":
        old_char_set = "'"
    if new_char == "CHR(39)":
        new_char = "'"

    if new_char is None or new_char == '':
        if case_sensitive:
            return re.sub('[' + re.escape(old_char_set) + ']', '', input_string)
        else:
            return re.sub('[' + re.escape(old_char_set) + ']', '', input_string, flags=re.IGNORECASE)

    new_char = str(new_char)[0]
    if case_sensitive:
        return re.sub('[' + re.escape(old_char_set) + ']', new_char, input_string)
    else:
        return re.sub('[' + re.escape(old_char_set) + ']', new_char, input_string, flags=re.IGNORECASE)

spark.udf.register("REPLACECHAR", replacechr)


# COMMAND ----------

#UDF for REPLACESTR
import re

def replacestr(case_flag, input_string, *args):
    if case_flag != 0:
        case_sensitive = True
    else:
        case_sensitive = False
    
    if input_string is None:
        return None
    
    args_list = list(args)
    
    new_string = args_list[-1] if args_list and args_list[-1] is not None else ""
    
    for i, arg in enumerate(args_list[:-1]):
        if arg is not None and arg != "":
            args_list[i] = re.sub(r"CHR\(\d+\)", lambda x: chr(int(x.group(0)[4:-1])), arg)
    
    if new_string == "NULL" or new_string == "":
        for old_string in args_list[:-1]:
            if old_string is not None and old_string != "":
                input_string = re.sub(re.escape(old_string), '', input_string, flags=re.IGNORECASE)
        return input_string

    if "CHR(" in new_string:
        new_string = re.sub(r"CHR\(\d+\)", lambda x: chr(int(x.group(0)[4:-1])), new_string)
    
    for i, old_string in enumerate(args_list[:-1]):
        if old_string is not None and old_string != "":
            args_list[i] = re.sub(r"CHR\(\d+\)", lambda x: chr(int(x.group(0)[4:-1])), old_string)
    
    for old_string in args_list[:-1]:
        if old_string is None or old_string == "":
            continue        
        if case_sensitive:
            input_string = re.sub(re.escape(old_string), new_string, input_string)
        else:
            input_string = re.sub(re.escape(old_string), new_string, input_string, flags=re.IGNORECASE)

    return input_string

spark.udf.register("REPLACESTR", replacestr)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- UDF for IS_NUMBER
# MAGIC CREATE FUNCTION IF NOT EXISTS IS_NUMBER(value STRING) RETURNS BOOLEAN CONTAINS SQL RETURN
# MAGIC SELECT 
# MAGIC     CASE
# MAGIC         WHEN value IS NULL THEN NULL
# MAGIC         WHEN TRY_CAST(value AS DOUBLE) IS NOT NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END;

# COMMAND ----------

#IS_DATE UDF
from datetime import datetime

def is_date(value, format=None):
    if value is None:
        return None
    
    common_formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y%m%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S.%f", "%Y%m%d %H:%M:%S.%f",
        "%d/%m/%Y %H:%M:%S.%f", "%m/%d/%Y %H:%M:%S.%f"
        ]
    
    if format is not None:
        try:
            datetime.strptime(value, format)
            return True
        except ValueError:
            return False
    else:
        for fmt in common_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                pass
        return False
    

spark.udf.register("IS_DATE",is_date)


# COMMAND ----------

# UDF for INSTR
def instr(input_string, search_value, start=1, occurrence=1, comparison_type=0):
    start = int(start)
    occurrence = int(occurrence)

    if comparison_type not in [0, 1]:
        comparison_type = 0

    if occurrence <= 0:
        return 0

    if occurrence == 1:
        if start >= 0:
            index = input_string.find(search_value, start - 1)
        else:
            index = input_string.rfind(search_value, start, len(input_string))
    else:
        indexes = [i for i in range(len(input_string)) if input_string.startswith(search_value, i)]
        if len(indexes) < occurrence:
            return 0        
        if start >= 0:
            index = indexes[occurrence - 1]
        else:
            index = indexes[-occurrence]

    return index + 1

spark.udf.register("INSTR", instr)


# COMMAND ----------


