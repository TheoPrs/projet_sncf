import os
import datetime
from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery

load_dotenv()

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

TABLE_ID = os.getenv("TABLE_ID_BATCH")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DESTINATION_BLOB = os.getenv("DESTINATION_BLOB")
raw_batch = (
    "circulation_id:STRING, ligne_id:STRING, date_circulation:DATE,"
    "gare_depart:STRING, gare_arrivee:STRING, heure_depart:TIME,"
    "heure_arrivee:TIME, retard:INTEGER, etat:STRING"
)
filename = f"gs://{BUCKET_NAME}/{DESTINATION_BLOB}"


def run():
    def parseclean(message):
        valeurs = message.split(",")
        data = dict(zip(header, valeurs))
        data["etat"] = data["etat"].replace("À", "A")
        data["date_circulation"] = datetime.datetime.strptime(
            data["date_circulation"], "%Y-%m-%d %H:%M:%S.%f"
        ).strftime("%Y-%m-%d")
        return data
    with beam.Pipeline() as p:
        (
            p
            | "Lire" >> ReadFromText(filename, skip_header_lines=1)
            | "Parse" >> beam.Map(parseclean)
            | "Filtrer" >> beam.Filter(
                lambda x: x["heure_depart"] < x["heure_arrivee"]
                and x["gare_depart"] != x["gare_arrivee"]
            )
            | "Ecrire" >> WriteToBigQuery(
                table=TABLE_ID,
                schema=raw_batch,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )


if __name__ == "__main__":
    run()
