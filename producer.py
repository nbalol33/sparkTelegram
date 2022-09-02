import time
from telethon.tl.functions.channels import JoinChannelRequest
from telethon import TelegramClient, events
from kafka import KafkaProducer

api_id = 13803428
api_hash = '4784f44a9f0f1a436c92f263fcb47cfe'

# Kafka topic name
TOPIC_NAME = "telegram15"

# Kafka server
KAFKA_HOST = "localhost"
KAFKA_PORT = "9092"

# Here you define the target channel that you want to listen to:
channels = ['https://t.me/tass_agency', 'https://t.me/rt_russian', 'https://t.me/interfaxonline',
            'https://t.me/vestiru24',
            'https://t.me/rentv_news', 'https://t.me/ntvnews', 'https://t.me/news_1tv', 'https://t.me/rian_ru']

client = TelegramClient('tref', api_id, api_hash)


class KafkaCommunicator:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def send(self, message):
        self.producer.send(self.topic, message.encode("utf-8"))

    def close(self):
        self.producer.close()


def create_communicator():
    """Create Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST + ":" + KAFKA_PORT)
    return KafkaCommunicator(producer, TOPIC_NAME)

communicator = create_communicator()

# Listen to messages from target channel
def main():
    @client.on(events.NewMessage(channels))
    async def newMessageListener(event):
        for channel in channels:
            await client(JoinChannelRequest(channel))
            time.sleep(5)

        message = event.message.message
        sender = await event.get_sender()
        username = sender.username

        messages = {'message': message, 'username': username}
        print(messages)
        communicator.send(str(messages))

    with client:
        client.run_until_disconnected()

if __name__ == '__main__':
    main()
