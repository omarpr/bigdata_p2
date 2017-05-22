# /opt/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 p2-words.py

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

kafkaStream = KafkaUtils.createStream(ssc, 'data04:2181', 'trump-consumer-group0', {'trump':1})

stopwords = ['a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'were', 'will', 'with']

dataJson = kafkaStream.map(lambda x: json.loads(x[1]))

messages = dataJson.map(lambda x: (x['text'].replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('.', '').replace(':', '').replace(',', '').replace('"', '').strip(), datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=timezone.utc)))
messages_downsecs = messages.map(lambda x: (x[0], x[1] - timedelta(seconds=x[1].second, microseconds=x[1].microsecond)))
words = messages_downsecs.flatMap(lambda x: list(map(lambda y: (y, x[1]), x[0].split(' '))))
words_clean = words.filter(lambda x: x[0].lower() not in stopwords and x[0] != '')
words_map = words_clean.map(lambda x: (x, 1))
words_reduce = words_map.reduceByKey(lambda x, y: x+y)

words_reduce.count().map(lambda x:'Words in this batch: %s' % x).pprint()

words_reduce.map(lambda x: ((x[0][0], x[0][1].isoformat()), x[1])).saveAsTextFiles('hdfs://master:9000/home/omar/p2/words.txt')

ssc.start()
ssc.awaitTermination()
