"""Creates and inserts data in bigquery.

References:
    https://docs.streamlit.io/knowledge-base/tutorials/databases/bigquery
    https://cloud.google.com/bigquery/docs/tables
    https://cloud.google.com/bigquery/docs/locations
    https://practicaldatascience.co.uk/data-engineering/how-to-import-data-into-bigquery-using-pandas-and-mysql
"""


import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd


credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


def create_dataset(dataset_id, region_name):
    reference = client.dataset(dataset_id)
    try:
        client.get_dataset(reference)
    except NotFound:
        dataset = bigquery.Dataset(reference)
        dataset.location = region_name
        dataset = client.create_dataset(dataset)


def insert(df, table):    
    return client.load_table_from_dataframe(df, table)


if __name__ == '__main__':
    data = {'Country': ['Philippines', 'Japan'], 'Capital': ['Manila', 'Tokyo']}
    df = pd.DataFrame(data)

    st.write('#### Dataframe to be imported to bigquery')
    st.dataframe(df)

    with st.form('form'):
        project_id = st.text_input('Project id')
        dataset_id = st.text_input('Dataset id')
        table_name = st.text_input('table name')
        region_name = st.text_input('Region name')
        submit = st.form_submit_button('Submit')

    if submit:
        create_dataset(dataset_id, region_name)
        tableid = f'{project_id}.{dataset_id}.{table_name}'
        insert(df, tableid)