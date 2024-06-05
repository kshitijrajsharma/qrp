import json

import duckdb
import requests
import streamlit as st

con = duckdb.connect()
con.sql("install spatial")
con.sql("load spatial")


def load_parquet_data(s3_parquet_url):
    with st.spinner("Loading Parquet data..."):
        con.execute(
            "CREATE OR REPLACE VIEW parquet_data AS SELECT * exclude geometry , ST_GeomFromWKB(geometry) as geom ,  FROM parquet_scan('{}')".format(
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


bbox_input = st.text_input("Enter Bounding Box (e.g., 'minx,miny,maxx,maxy'):", "")

if s3_parquet_url:
    load_parquet_data(s3_parquet_url)
    st.write("Remote Table:")
    schema = con.execute("DESCRIBE parquet_data").df()
    st.dataframe(schema)

    if bbox_input:
        try:
            minx, miny, maxx, maxy = map(float, bbox_input.split(","))
            geometry = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [minx, miny],
                        [minx, maxy],
                        [maxx, maxy],
                        [maxx, miny],
                        [minx, miny],
                    ]
                ],
            }

            def fetch_data(
                geometry,
                url="https://api-prod.raw-data.hotosm.org/v1/stats/polygon/",
                max_retries=2,
            ):
                headers = {"Content-Type": "application/json"}
                geom_dump = json.dumps({"geometry": geometry})

                for _ in range(max_retries):
                    try:
                        response = requests.post(url, data=geom_dump, headers=headers)
                        response.raise_for_status()
                        return response.json()
                    except requests.exceptions.RequestException as e:
                        st.warning(f"Error fetching data: {e}")
                        continue

                st.error("Failed to fetch data after multiple retries.")
                return None

            data = fetch_data(geometry)
            if data:
                st.write(data["raw"])
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS poly_stats (
                        population INTEGER,
                        populatedAreaKm2 DOUBLE,
                        osmBuildingsCount INTEGER,
                        osmHighwayLengthKm DOUBLE,
                        buildingCount6Months INTEGER,
                        highwayLength6MonthsKm DOUBLE
                    )
                """
                )

                con.execute(
                    """
                    INSERT INTO poly_stats VALUES (
                        {population}, {populatedAreaKm2}, {osmBuildingsCount}, {osmHighwayLengthKm},
                        {buildingCount6Months}, {highwayLength6MonthsKm}
                    )
                """.format(
                        population=data["raw"]["population"],
                        populatedAreaKm2=data["raw"]["populatedAreaKm2"],
                        osmBuildingsCount=data["raw"]["osmBuildingsCount"],
                        osmHighwayLengthKm=data["raw"]["osmHighwayLengthKm"],
                        buildingCount6Months=data["raw"]["buildingCount6Months"],
                        highwayLength6MonthsKm=data["raw"]["highwayLength6MonthsKm"],
                    )
                )

                st.success("Data inserted into poly_stats table.")

        except ValueError:
            st.error("Invalid bounding box format. Please enter 'minx,miny,maxx,maxy'.")

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
                    sum(count) AS total
                FROM
                    aggregated_data
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
