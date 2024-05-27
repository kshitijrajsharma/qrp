import duckdb
import streamlit as st

con = duckdb.connect()


def load_parquet_data(s3_parquet_url):
    with st.spinner("Loading Parquet data..."):
        con.execute(
            "CREATE OR REPLACE VIEW parquet_data AS SELECT * FROM parquet_scan('{}')".format(
                s3_parquet_url
            )
        )


s3_parquet_url = st.text_input(
    "Enter the S3 Parquet file URL:",
    "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/argentina/parquet/buildings.geo.parquet",
)

if s3_parquet_url:
    load_parquet_data(s3_parquet_url)

    st.write("Remote Table:")
    schema = con.execute("DESCRIBE parquet_data").df()
    st.dataframe(schema)

    query = st.text_area("Enter SQL query:", "SELECT * FROM parquet_data LIMIT 10")

    if st.button("Run Query"):
        if query.strip():
            try:
                with st.spinner("Running query..."):
                    df = con.execute(query).df()
                st.dataframe(df)
            except Exception as ex:
                st.error(ex)
        else:
            st.warning("Please enter a valid SQL query.")
else:
    st.warning("Please enter a valid S3 Parquet file URL.")
