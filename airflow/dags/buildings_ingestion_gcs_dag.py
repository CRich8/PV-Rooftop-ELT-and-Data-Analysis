# FIXME: This script needs a docstring explaining what it does/how it's used
import os
# Removed unused import

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago  # Sorted import alphabetically

# Removed unused imports
from datetime import datetime, timedelta
from google.cloud import storage  # Sorted alphabetically

# Moved from down below
from bs4 import BeautifulSoup
import re
import requests

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'pv_rooftop_data_all')

# "Magic numbers" are discouraged within code - these are numbers that appear with little context and are seemingly
# arbitrary, such as your file and chunk sizes. Any numbers that are hard-coded should be declared as global variables
# and referenced by name in the function calls. The main reason for this is that in the future, this number could very
# well change, and you want to make it as easy as possible for yourself or someone else to change them. You don't want
# to have to track down all the usages within the code itself (you could imagine that a number might be used more than
# once), which would get messy in a large file or codebase. If you want to be really pro, declare a separate
# `constants.py` file that contains all of your constants for all of your files in this module, and then import the
# specific constants you need for the file in your import statements (i.e., `from constants import MAX_LINKS`)!
MAX_MULTIPART_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_LINKS = 23


# PEP-8 guidelines encourage two spaces before global function definitions
# (Comment lines count as the first line of a definition)
def upload_to_gcs(bucket, object_name, local_file):
    # FIXME: docstring should provide an explanation of what the function does. Since this function does not have a
    #  return, you can leave the `:return:` out
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    storage.blob._MAX_MULTIPART_SIZE = MAX_MULTIPART_FILE_SIZE
    storage.blob._DEFAULT_CHUNKSIZE = DEFAULT_CHUNK_SIZE

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="buildings_ingestion_gcs_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
    schedule_interval="@daily",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=15)
):  # Because you don't use `dag` in any way, you don't have to declare it using `as`

    # You know this code is guaranteed to run when this script is executed, so put these imports above with the rest of
    # your import statements. I think it's discouraged behavior to put import statements inside of function calls,
    # unless the imports are really big and you don't guarantee that the function is ever going to be called at all in
    # the lifetime of the program
    url = 'https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pv-rooftop/buildings/'
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    count = 0

    # I'm not familiar with BeautifulSoup but this search feels extremely generic/broad and I'm wondering if whatever
    # you're trying to do here can be done in a better/cleaner/prettier way than just searching for the letter 'a'?
    for link in soup.find_all('a'):
        url = 'https://data.openei.org/' + link.get('href', "")  # is it guaranteed that .get will return a string here?
                                                                 # If not, add a default value to the call that it will
                                                                 # fall back to
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        soup = soup.find('tr', class_="odd")
        # What's happening here? Why is this in a `while` loop? Can't you just put the `try` and `finally` here as-is?
        # Using `while True` is really risky because you could create an infinite loop. It's generally reserved for
        # running a program from __main__. Here, it _seems_ like you're trying to just do a single action, the action
        # happens, and then you exit the loop, so I don't understand why you're in a loop at all.
        while True:
            try:
                url = soup.find('a')['href']
                urls.append(url)
            finally: 
                break
        count += 1
        if count == MAX_LINKS:
            break

    urls = [s for s in urls if s.startswith('h')]  # It seems like you're trying to just get any URLs that start with
                                                   # http. You could maybe eliminate this step by having more specific
                                                   # checks and criteria in your `for` loop above, so you only add a
                                                   # link to the `urls` list if it's actually a list you want, rather
                                                   # than making a list and then making another more refined list

    urls_and_years = []
    for url in urls:
        city_year_matches = re.search('city_year=(.*)/', url)  # the `search` function's return type is an `Optional`,
                                                              # which means its value could be `none`. If that were the
                                                              # case, then `city_year_results`'s value would be `none`
                                                              # and the next line would throw an exception, so you need
                                                              # to either first check whether it's `none` or you need
                                                              # to put the next line in a try/except block and handle
                                                              # the exception gracefully
        city_year = city_year_matches.group(1)  # I renamed the variable on the above line. I don't love reassigning a
                                                # variable's value based on itself, and I don't think the initial
                                                # variable's name was an accurate representation of what value it
                                                # actually held. It held a `[Match[AnyStr]]` list, not a year value.

        """
        Dictionaries are for when you need to store things to be able to access uniquely and quickly later, and aren't 
        planning on just iterating through them one entry at a time. The advantage of a dictionary is that it allows you 
        to find a value for a given key in much faster time than iterating through a list to find it. You're not using 
        the dictionary structure here for its intended purpose. It's a little odd to me that you're using the url itself
        as the key in the dictionary. It *SEEMS* like the reason you're doing this is so you can get the url and year 
        later on as a tuple in the `for` loop you create, so I've modified your approach to instead just create a
        list of tuples to iterate through. I'm pretty sure what I've done still accomplishes what you were doing before,
        but it would be worth double checking me on that. I could have made a mistake. Which, actually, is also a good 
        segue into the value of unit testing - if had written a good unit test for this script, you could instantly 
        test this new version of the code with the same unit test to ensure that nothing fundamentally has changed!
        """
        urls_and_years.append((url, city_year))

    for url, city_year in urls_and_years:
        OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f"/output_buildings_{city_year}.parquet"
        TABLE_NAME = f"buildings_{city_year}"
        PARQUET_FILE = f"buildings_{city_year}.parquet"
        download_dataset_task = BashOperator(
            task_id=f"download_buildings_{city_year}_dataset_task",
            bash_command=f"curl -sSLf {url} > {OUTPUT_FILE_TEMPLATE}"
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_buildings_{city_year}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"buildings/{PARQUET_FILE}",
                "local_file": f"{OUTPUT_FILE_TEMPLATE}",
            },
        )

        rm_task = BashOperator(
            task_id=f"rm_buildings_{city_year}_task",
            bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
        )

        # FIXME: It's unclear to me what's happening here. the `>>` operator is a bitwise operator, but this doesn't _look_
        #  like a bitwise operation to me, and my IDE is calling it out as an operation with no used effect. I'm assuming
        #  it does something otherwise you wouldn't have put it here but I'm curious
        download_dataset_task >> local_to_gcs_task >> rm_task

