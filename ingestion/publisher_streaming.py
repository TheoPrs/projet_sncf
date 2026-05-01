import json
from mock_generator import mock_generator
from google.cloud import pubsub_v1
from dataclasses import asdict
import time
from dotenv import load_dotenv
import os

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
gen = mock_generator()

for _ in range(400): 
  circulation = next(gen)
  circulation_dict = asdict(circulation)
  data = json.dumps(circulation_dict, default=str, ensure_ascii=False).encode('UTF-8')
  future = publisher.publish(topic_path,data)
  future.result()
  print("Message publié avec succès !")
  time.sleep(1)