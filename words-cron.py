# pyspark --master yarn --deploy-mode client --conf='spark.executorEnv.PYTHONHASHSEED=223'
# /opt/spark/bin/spark-submit words-cron.py

from pyspark import SparkContext
from datetime import datetime, timezone, timedelta
import calendar, time, os, json

sc = SparkContext(appName="p2-words-cron")
sc.setLogLevel("WARN")

def resultToFiles(drd, file_dir, data_name, time_freq):
    out = {}

    f_index = file_dir + data_name + '-index.json'
    f_index_change = False
    ws_index = {}

    if (time_freq == '1h'):
        freq = timedelta(hours=1)
    elif (time_freq == '10m'):
        freq = timedelta(minutes=10)
    else:
        freq = timedelta()

    if (os.path.isfile(f_index)):
        with open(f_index, 'r') as f:
             ws_index = json.load(f)

    for x in drd.collect():
        ts = int(time.mktime(x[0][1].timetuple()))
        if (ts not in out):
            out[ts] = []
        out[ts].append((x[0][0], x[1]))

    for x in out:
        f = file_dir + data_name + '.txt-' + str(x)

        if (os.path.isfile(f)):
            os.remove(f)

        target_file = os.open(f, os.O_RDWR|os.O_CREAT)
        os.write(target_file, str.encode(json.dumps(out[x])))
        os.close(target_file)

        if (x not in ws_index):
            tstime = datetime.fromtimestamp(x)
            drange = tstime.strftime('%d-%b-%Y %I:%M%p') + ' - ' + (tstime + freq).strftime('%d-%b-%Y %I:%M%p')
            f_index_change = True
            ws_index[x] = {'ts': x, 'file': data_name + '.txt-' + str(x), 'date_range': drange}


    if (f_index_change):
        if (os.path.isfile(f_index)):
            os.remove(f_index)

        f = open(f_index, 'w+')
        f.write(json.dumps(ws_index))
        f.close()

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

resultToFiles(e4a_reduce, main_dir, 'hashtags', '1h')
resultToFiles(e4b_reduce, main_dir, 'keywords', '1h')
resultToFiles(e5_reduce, main_dir, 'words_selection', '1h')
