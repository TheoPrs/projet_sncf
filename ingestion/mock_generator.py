import random
from dataclasses import dataclass, asdict
import uuid
import datetime
import time

retard = [0,5,10,20,30]
weight = [70,5,10,10,5]
gares = ["Lille", "Paris", "Marseille","Lyon","Roubaix","Nice","Tourcoing","Douai","Arras","Valenciennes"]
lignes = ["TER","TGV","Intercite"]

@dataclass
class circulation:
  circulation_id:uuid
  ligne_id:str
  date_circulation:datetime
  gare_depart:str
  gare_arrivee:str
  heure_depart:str
  heure_arrivee:str
  retard:int
  etat:str

def mock_generator():
 while True : 
    depart_heure = random.randint(0,23)
    depart_minute = random.randint(0,59)
    if depart_minute == 59:
        depart_minute = random.randint(0, 58)
    arrivee_heure = random.randint(depart_heure,23)
    if depart_heure == arrivee_heure:
        arrivee_minute = random.randint(depart_minute+1,59)
    else:
        arrivee_minute = random.randint(0,59)
           
    depart_gare = random.choice(gares)
    arrivee_gare = random.choice(gares)
    while arrivee_gare == depart_gare:
     arrivee_gare = random.choice(gares)
    ret = random.choices(retard,weight)[0]

    obj = circulation(
    circulation_id = uuid.uuid4(),
    ligne_id= f"{random.choice(lignes)}_{random.randint(0,100)}",
    date_circulation=datetime.datetime.now(),
    gare_depart= depart_gare,
    gare_arrivee= arrivee_gare,
    heure_depart= datetime.time(depart_heure,depart_minute),
    heure_arrivee= datetime.time(arrivee_heure,arrivee_minute),
    retard=ret,
    etat= "À l'heure" if (ret < 5)  else "En retard"
    
    )

    yield obj
