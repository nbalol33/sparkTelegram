from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pymorphy2
import glob
import os.path

SPARK_APP_NAME = "KafkaWordCount"
SPARK_CHECKPOINT_TMP_DIR = "file:////home/ubuntu/spark3/output"
SPARK_BATCH_INTERVAL = 30
SPARK_LOG_LEVEL = "OFF"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telegram15"

with open('stop-words-russian.txt') as f:
    stopwords = f.read()

def create_streaming_context(SPARK_BATCH_INTERVAL):
    sc = SparkContext(appName=SPARK_APP_NAME)
    sc.setLogLevel(SPARK_LOG_LEVEL)
    ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
    ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)
    return ssc

def update_total_count(current_count, count_state):
    if count_state is None:
        count_state = 0
    return sum(current_count, count_state)

def create_stream(ssc):
    return (
        KafkaUtils.createDirectStream(
            ssc, topics=[KAFKA_TOPIC],
            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            .map(lambda x: x[1])
    )

def getterUser(info):
    return eval(info)["username"]


def getterMessage(info):
    return eval(info)["message"]


def fstopwords(word):
    if word.lower() not in stopwords:
        return True
    else:
        return False


def normalize(word):
    if fstopwords(word):
        morph = pymorphy2.MorphAnalyzer()
        word = word.strip("\n\n").strip(".").strip(",").strip(":")
        if word[0].isupper():
            word = morph.parse(word)[0]
            word = word.normal_form
            word = word.capitalize()
            return word
        else:
            return None
    else:
        return None

def main():
    try:
        ssc = create_streaming_context(60)
        messages = create_stream(ssc)
        total_counts_sorted = (
            messages
                .map(getterMessage)
                .flatMap(lambda line: line.split())
                .map(normalize)
                .filter(lambda x: x is not None)
                .map(lambda word: (word, 1))
                .reduceByKey(lambda x, y: x + y)
                .updateStateByKey(update_total_count)
                .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
            .saveAsTextFiles('file:////home/ubuntu/spark3/upperW/upperW')
        )
        ssc.start()
        ssc.awaitTermination()

    except KeyboardInterrupt:
        folder_path = r'/home/ubuntu/spark3/upperW'
        files = glob.glob(folder_path + '/upperW-*' + '/part-00000')
        max_file = max(files, key=os.path.getctime)
        #print(max_file)
        with open(max_file, 'r') as myfile:
            words = myfile.read().split('\n')
            print("Top 10 used words:")
            for x in range(10):
                print(words[x])


if __name__ == "__main__":
    main()