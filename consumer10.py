from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SPARK_APP_NAME = "KafkaWordCount"
SPARK_CHECKPOINT_TMP_DIR = "file:////home/ubuntu/spark3/output"
SPARK_BATCH_INTERVAL = 30
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telegram8"

def create_streaming_context(SPARK_BATCH_INTERVAL):
    sc = SparkContext(appName=SPARK_APP_NAME)
    sc.setLogLevel(SPARK_LOG_LEVEL)
    ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
    ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)
    return ssc

def create_stream(ssc):
    return (
        KafkaUtils.createDirectStream(
            ssc, topics=[KAFKA_TOPIC],
            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            .map(lambda x: x[1])
    )

def getter(info):
    return eval(info)["username"]

#def natasha():

def main():
    ssc = create_streaming_context(SPARK_BATCH_INTERVAL)
    messages = create_stream(ssc)
    total_counts_sorted = (
        messages
            .map(getter)
            .map(lambda word: (word, 1))
            .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 600, 30)
            .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
    )
    total_counts_sorted.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()