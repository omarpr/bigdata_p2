# pyspark --master yarn --deploy-mode client --conf='spark.executorEnv.PYTHONHASHSEED=223'
# /opt/spark/bin/spark-submit screennames-cron.py

from pyspark import SparkContext
from datetime import datetime, timezone, timedelta
import calendar, time, os, json
from p2lib import *

sc = SparkContext(appName="p2-screennames-cron")
sc.setLogLevel("WARN")

main_dir = '/home/omar.soto2/p2/files/'

a = sc.textFile('/home/omar/p2/screen_names.txt-*/part-*')
b = a.map(lambda x: eval(x))
c = b.map(lambda x: ((x[0][0].upper(), (datetime.strptime(x[0][1].split('+')[0], '%Y-%m-%dT%H:%M:%S') - timedelta(seconds=time.timezone))), x[1]))

map_12h = c.map(lambda x: ((x[0][0], x[0][1] - timedelta(minutes=x[0][1].minute) - timedelta(hours=12)), x[1]))

e4c = map_12h
e4c = e4c.filter(lambda x: (x[0][1] + timedelta(seconds=time.timezone)) >= datetime.utcnow() - timedelta(hours=24))
e4c_reduce = e4c.reduceByKey(lambda x, y: x+y)

print("Count: " + str(e4c.count()))

resultToFiles(e4c_reduce, main_dir, 'screen_names', '12h')
