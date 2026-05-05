import pandas as pd
from google.cloud import storage
import os 
from dotenv import load_dotenv
from io import StringIO
import great_expectations as gx

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
DESTINATION_BLOB = os.getenv("DESTINATION_BLOB")
expectations_suite_name = "import_suite"


def run():
  client = storage.Client()
  bucket = client.get_bucket(BUCKET_NAME)
  blob = bucket.blob(DESTINATION_BLOB)
  data = blob.download_as_text()
  df = pd.read_csv(StringIO(data))
  context = gx.get_context()
  ds = context.data_sources.add_pandas("pandas_source")
  da = ds.add_dataframe_asset("circulations")
  batch_def = da.add_batch_definition_whole_dataframe("batch")
  batch = batch_def.get_batch(batch_parameters={"dataframe": df})
  suite = gx.ExpectationSuite(name=expectations_suite_name)
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="circulation_id")
  )
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="circulation_id")
  )
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="retard",min_value=0,max_value=30)
  )
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(column="etat",value_set=["a_l_heure","en_retard"])
  )
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(column="date_circulation",regex="^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$")
  )
  suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(column="ligne_id",regex="^(TER|TGV|Intercite)_\d+$")
  )

  results = batch.validate(suite)
  print(results)

if __name__ == "__main__" : run()