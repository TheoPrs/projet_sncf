import csv
import os
from dotenv import load_dotenv
from dataclasses import asdict
from mock_generator import mock_generator
from google.cloud import storage

load_dotenv()
gen = mock_generator()
header = [
          "circulation_id",
          "ligne_id",
          "date_circulation",
          "gare_depart",
          "gare_arrivee",
          "heure_depart",
          "heure_arrivee",
          "retard",
          "etat",
        ]
data = []
BUCKET_NAME = os.getenv("BUCKET_NAME")
DESTINATION_BLOB = os.getenv("DESTINATION_BLOB")
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

for _ in range(100):
    circulation = next(gen)
    data.append(asdict(circulation).values())

with open("circulations.csv", 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(data)


blob = bucket.blob(DESTINATION_BLOB)
blob.upload_from_filename("circulations.csv")
