import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import datetime

PROJECT_ID = "gcpsncf"
TOPIC_ID = "sncf-circulations"
DATASET_ID = "sncf_dataset"
TABLE_ID = "gcpsncf.sncf_dataset.raw_streaming"
topic = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"

raw_streaming = "circulation_id:STRING, ligne_id:STRING, date_circulation:DATE ,gare_depart:STRING, gare_arrivee:STRING, heure_depart:TIME, heure_arrivee:TIME, retard:INTEGER, etat:STRING"
def parse_clean(message):
    data = json.loads(message.decode('UTF-8'))
    data["etat"] = data["etat"].replace("À", "A")
    data["date_circulation"] = datetime.datetime.strptime(data["date_circulation"],"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d")
    print(data)
    return data

options = beam.options.pipeline_options.PipelineOptions(streaming=True)
with beam.Pipeline(options=options) as p:
  result = (p 
     | "Lire" >> ReadFromPubSub(topic=topic)
     | "Parser" >> beam.Map(parse_clean)
     | "Filtrer" >> beam.Filter(lambda x: print(x["heure_depart"], x["heure_arrivee"]) or x["heure_depart"] < x["heure_arrivee"] and x["gare_depart"] != x["gare_arrivee"])
     | "Ecrire" >> WriteToBigQuery(
         table=TABLE_ID,
         schema = raw_streaming,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         method='STREAMING_INSERTS'
         )
      
    )
  result['FailedRows'] | "Erreurs" >> beam.Map(print)
    