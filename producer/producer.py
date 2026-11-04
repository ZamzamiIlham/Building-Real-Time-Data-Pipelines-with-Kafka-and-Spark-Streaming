import json
import random
import time
from datetime import timedelta,timezone,datetime

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'transactions'

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "transaction-producer"
})

users = [f"user_{i}" for i in range(1,100)]
VALID_SOURCES = ["mobile", "web", "pos"]

#make probablitiy invalid data 70%,20% dan 10%
EVENT_TYPES = ["valid"] * 70 + ["invalid"] * 20 + ["late"] * 10
print(EVENT_TYPES)

#ini akan menjadi sebuah list array berisi 100 event_types

invalid_types = [
    "amount_negative",
    "amount_too_high",
    "bad_timestamp",
    "missing_user_id",
]

def generate_report(err,msg):
    if err is not None:
        print(f"[Erros] => {err}")
    else:
        print(
            f"Delivered topic = {msg.topic()} ",
            f"Partition = {msg.partition()} ",
            f"Offset = {msg.offset()}"
        )


def generate_valid_event():
    time_now = datetime.now()

    data = {
        'user_id': random.choice(users),
        'amount' : random.randint(10000,200000),
        'timestamp': time_now.strftime("%Y-%m-%d %H:%M:%S"),
        'source': random.choice(VALID_SOURCES)
    }

    return data


def generate_invalid_types():

    event = generate_valid_event()
    error_type = random.choice(invalid_types)

    if error_type == "amount_negative":
        event['amount'] = random.randint(-50000,-1000)
    elif error_type == "amount_too_high":
        event['amount'] = random.randint(9000000,9900000)
    elif error_type == "bad_timestamp":
        event['timestamp'] = "1999-20-30"
    elif error_type == "missing_user_id":
        event.pop('user_id', None)

    return event,error_type


def generate_late_types():
    event = generate_valid_event()

    late_time = datetime.now(timezone.utc) - timedelta(minutes=5)

    event['timestamp'] = late_time.strftime("%Y-%m-%d %H:%M:%S")

    return event

def main():
    print("Mengirim data ke kafka")

    try:
        while True:

            type_event = random.choice(EVENT_TYPES)

            if type_event == 'valid':
                data = generate_valid_event()
            elif type_event == 'invalid':
                data, error_type = generate_invalid_types()
            else:
                data = generate_late_types()

            #tentukan key
            key =str(data.get('user_id', 'UNKNOWN'))
            payload = json.dumps(data)

            producer.produce(
                topic=TOPIC_NAME,
                key=key,
                value=payload,
                on_delivery=generate_report
            )

            producer.poll(0)
            #kirim data 1-2 detik
            time.sleep(random.uniform(1.0,2.0))
    except KeyboardInterrupt:
        print("Producer berhenti")
    finally:
        producer.flush()

if __name__ == '__main__':
    main()






    