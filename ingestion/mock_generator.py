import json
import random
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

# Données de référence
LIGNES = ["TER Nord", "TER Hauts-de-France", "Intercités", "TGV"]
GARES = ["Lille-Flandres", "Arras", "Amiens", "Valenciennes", "Douai", "Lens", "Béthune"]

PROJECT_ID = "projet_sncf"
TOPIC_ID = "line_raw"

def generate_train():
    depart = random.choice(GARES)
    arrivee = random.choice([g for g in GARES if g != depart])
    retard = random.randint(-5, 120) 

    return {
        "train_id": f"TER_{random.randint(1000, 9999)}",
        "ligne_id": random.choice(LIGNES),
        "gare_depart": depart,
        "gare_arrivee": arrivee,
        "date_circulation": datetime.now().strftime("%Y-%m-%d"),
        "heure_depart": (datetime.now() + timedelta(minutes=random.randint(0, 1440))).strftime("%H:%M:%S"),
        "retard_minutes": retard,
        "statut": "supprime" if random.random() < 0.02 else "circule"
    }

def publish_batch(n=100):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
  for _ in range (n):
    train = generate_train()
    data = json.dumps(train).encode("UTF-8")
    future = publisher.publish(topic_path,data)
    future.result()
    print(f"Publié : {train['train_id']}")

if __name__ == "__main__":
    batch = publish_batch(100)