# /opt/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 p2-screennames.py

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json

sc = SparkContext(appName="KafkaSparkStream-p2-words")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createStream(ssc, 'data04:2181', 'trump-consumer-group1', {'trump':1})

dataJson = kafkaStream.map(lambda x: json.loads(x[1]))

names = dataJson.map(lambda x: (x['user']['screen_name'], datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=timezone.utc)))
names_downsecs = names.map(lambda x: (x[0], x[1] - timedelta(seconds=x[1].second, microseconds=x[1].microsecond)))
names_map = names_downsecs.map(lambda x: (x, 1))
names_reduce = names_map.reduceByKey(lambda x, y: x+y)

names_reduce.count().map(lambda x:'Screen Names in this batch: %s' % x).pprint()

names_reduce.map(lambda x: ((x[0][0], x[0][1].isoformat()), x[1])).saveAsTextFiles('hdfs://master:9000/home/omar/p2/screen_names.txt')

ssc.start()
ssc.awaitTermination()
