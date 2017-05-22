# pyspark --master yarn --deploy-mode client --conf='spark.executorEnv.PYTHONHASHSEED=223'
# /opt/spark/bin/spark-submit words-cron.py

from pyspark import SparkContext
from datetime import datetime, timezone, timedelta
import calendar, time, os, json
from p2lib import *

sc = SparkContext(appName="p2-words-cron")
sc.setLogLevel("WARN")

main_dir = '/home/omar.soto2/p2/files/'
words_selection = ['TRUMP', 'MAGA', 'DICTATOR', 'IMPEACH', 'DRAIN', 'SWAMP']

a = sc.textFile('/home/omar/p2/words.txt-*/part-*')
b = a.map(lambda x: eval(x))
c = b.map(lambda x: ((x[0][0].upper(), (datetime.strptime(x[0][1].split('+')[0], '%Y-%m-%dT%H:%M:%S') - timedelta(seconds=time.timezone))), x[1]))

map_10m = c.map(lambda x: ((x[0][0], x[0][1] - timedelta(minutes=x[0][1].minute % 10)), x[1]))
map_1h = c.map(lambda x: ((x[0][0], x[0][1] - timedelta(minutes=x[0][1].minute)), x[1]))

e4a = map_1h
e4a = e4a.filter(lambda x: (x[0][1] + timedelta(seconds=time.timezone)) >= datetime.utcnow() - timedelta(hours=2))
e4a_filter = e4a.filter(lambda x: x[0][0][0] == '#')
e4a_reduce = e4a_filter.reduceByKey(lambda x, y: x+y)

e4b = map_1h
e4b = e4b.filter(lambda x: (x[0][1] + timedelta(seconds=time.timezone)) >= datetime.utcnow() - timedelta(hours=2))
e4b_filter = e4b.filter(lambda x: x[0][0][0] != '#')
e4b_reduce = e4b_filter.reduceByKey(lambda x, y: x+y)

e5 = map_1h
e5 = e5.filter(lambda x: (x[0][1] + timedelta(seconds=time.timezone)) >= datetime.utcnow() - timedelta(hours=2))
e5_filter = e5.filter(lambda x: x[0][0] in words_selection)
e5_reduce = e5_filter.reduceByKey(lambda x, y: x+y)

resultToFiles(e4a_reduce, main_dir, 'hashtags', '1h', 10)
resultToFiles(e4b_reduce, main_dir, 'keywords', '1h', 10)
resultToFiles(e5_reduce, main_dir, 'words_selection', '1h', len(words_selection))
