import os
import json
import datetime
from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")
TABLE_ID = os.getenv("TABLE_ID")
topic = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"
raw_streaming = (
    "circulation_id:STRING, ligne_id:STRING, date_circulation:DATE,"
    "gare_depart:STRING, gare_arrivee:STRING, heure_depart:TIME,"
    "heure_arrivee:TIME, retard:INTEGER, etat:STRING"
)


def parse_clean(message):
    data = json.loads(message.decode("UTF-8"))
    data["etat"] = data["etat"].replace("À", "A")
    data["date_circulation"] = datetime.datetime.strptime(
        data["date_circulation"], "%Y-%m-%d %H:%M:%S.%f"
    ).strftime("%Y-%m-%d")
    return data


options = beam.options.pipeline_options.PipelineOptions(streaming=True)
with beam.Pipeline(options=options) as p:
    result = (
        p
        | "Lire" >> ReadFromPubSub(topic=topic)
        | "Parser" >> beam.Map(parse_clean)
        | "Filtrer" >> beam.Filter(
            lambda x: x["heure_depart"] < x["heure_arrivee"]
            and x["gare_depart"] != x["gare_arrivee"]
        )
        | "Ecrire" >> WriteToBigQuery(
            table=TABLE_ID,
            schema=raw_streaming,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )
    )
