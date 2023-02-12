"""Query shakespeare sample using google bigquery.

Reference:
    https://docs.streamlit.io/knowledge-base/tutorials/databases/bigquery
"""

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery


# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


if __name__ == '__main__':
    st.subheader('Streamlit Google Bigquery Example')
    st.write('Query word from google\'s shakespeare data')

    is_query = st.button('Query')
    if is_query:
        rows = run_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")

        # Print results.
        st.write("Some wise words from Shakespeare:")
        for row in rows:
            st.write("✍️ " + row['word'])