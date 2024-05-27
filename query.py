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


parquet_url_query_options = {
    "Argentina Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/argentina/parquet/buildings.geo.parquet",
    "Indonesia Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/indonesia/parquet/buildings.geo.parquet",
    "Liberia Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/liberia/parquet/buildings.geo.parquet",
    "Nigeria Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/nigeria/parquet/buildings.geo.parquet",
    "Kenya Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/kenya/parquet/buildings.geo.parquet",
    "Malawi Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/malawi/parquet/buildings.geo.parquet",
    "Nepal Buildings": "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0/nepal/parquet/buildings.geo.parquet",
}
query_choice_parquet = st.selectbox(
    "Choose existing parquet files :", options=list(parquet_url_query_options.keys())
)

s3_parquet_url = st.text_area(
    "Enter Parquet URL:", parquet_url_query_options[query_choice_parquet]
)


if s3_parquet_url:
    load_parquet_data(s3_parquet_url)

    st.write("Remote Table:")
    schema = con.execute("DESCRIBE parquet_data").df()
    st.dataframe(schema)

    query_options = {
        "Select": "SELECT * FROM parquet_data LIMIT 10",
        "Get Stats by Dataset": """
            WITH unnested_data AS (
                SELECT
                    unnest(sources).dataset AS dataset,
                    unnest(sources).confidence AS confidence
                FROM
                    parquet_data
            ),
            aggregated_data AS (
                SELECT
                    dataset,
                    COUNT(*) AS count
                FROM
                    unnested_data
                GROUP BY
                    dataset
            ),
            total_count AS (
                SELECT
                    COUNT(*) AS total
                FROM
                    unnested_data
            )
            SELECT
                ad.dataset,
                ad.count,
                (ad.count * 100.0 / tc.total) AS percentage
            FROM
                aggregated_data ad,
                total_count tc;
        """,
    }

    query_choice = st.selectbox(
        "Choose a query to run:", options=list(query_options.keys())
    )

    query = st.text_area("Enter SQL query:", query_options[query_choice])

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
