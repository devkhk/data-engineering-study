
# timezone parser
# https://stackoverflow.com/questions/969285/how-do-i-translate-an-iso-8601-datetime-string-into-a-python-datetime-object

import pytz
import datetime

print(str(datetime.datetime.now()))
print(str(datetime.datetime.utcnow()))

str_iso_8061 = '2022-05-24T07:17:59.000Z'

parser_time = datetime.datetime.strptime(str_iso_8061, "%Y-%m-%dT%H:%M:%S.%fZ")
print("parser", parser_time)

utc_now = pytz.utc.localize(parser_time)
print(utc_now)
kst_now = utc_now.astimezone(pytz.timezone("ASIA/Seoul"))
dt = kst_now.strftime("%Y-%m-%d %H:%M:%S.%f")
print(dt)
