# pyspark --master yarn --deploy-mode client --conf='spark.executorEnv.PYTHONHASHSEED=223'
# /opt/spark/bin/spark-submit screennames-cron.py
# /opt/spark/bin/spark-submit --master yarn  --deploy-mode client --py-files /home/omar.soto2/p2/p2lib.py --conf='spark.executorEnv.PYTHONHASHSEED=223' /home/omar.soto2/p2/screennames-cron.py

from pyspark import SparkContext
from datetime import datetime, timezone, timedelta
import calendar, time, os, json
from p2lib import *

SparkContext.setSystemProperty('spark.executor.memory', '3G')

sc = SparkContext(appName="p2-screennames-cron")
sc.setLogLevel("WARN")

main_dir = '/home/omar.soto2/p2/files/'

a = sc.textFile('/home/omar/p2/screen_names.txt-*/part-*')
b = a.map(lambda x: eval(x))
c = b.map(lambda x: ((x[0][0].upper(), (datetime.strptime(x[0][1].split('+')[0], '%Y-%m-%dT%H:%M:%S') - timedelta(seconds=time.timezone))), x[1]))

map_1h = c.map(lambda x: ((x[0][0], x[0][1] - timedelta(minutes=x[0][1].minute)), x[1]))
map_reflect = map_1h.map(lambda x: reflect(x, 12)) # Reflect this occurance in the next 12 hours, so we can have the occurance on each range file (1-13, 2-14, 3-15, etc)
map_flat = map_reflect.flatMap(lambda x: x)

e4c = map_flat
e4c = e4c.filter(lambda x: (x[0][1] + timedelta(seconds=time.timezone)) >= datetime.utcnow() - timedelta(hours=24))
e4c_reduce = e4c.reduceByKey(lambda x, y: x+y)

resultToFiles(e4c_reduce, main_dir, 'screen_names', '12h', 10)
