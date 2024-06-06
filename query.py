import json
import os

import duckdb
import requests
import streamlit as st

con = duckdb.connect()
con.sql("install spatial")
con.sql("load spatial")


def load_parquet_data(s3_parquet_url):
    with st.spinner("Loading Parquet data..."):
        con.execute(
            "CREATE OR REPLACE VIEW parquet_data AS SELECT * exclude geometry , ST_GeomFromWKB(geometry) as geom FROM parquet_scan('{}')".format(
                s3_parquet_url
            )
        )


base_url = "s3://staging-raw-data-api/default/overture/2024-05-16-beta.0"

countries = ["Argentina", "Indonesia", "Kenya", "Liberia", "Malawi", "Nepal", "Nigeria"]

datasets = [
    "buildings",
    "boundary",
    "land_cover",
    "land_use",
    "land",
    "placenames",
    "places",
    "roads",
    "water",
]

country_bboxes = {
    "Argentina": "-73.419999,-55.224869000000005,-53.630001,-21.8306177",
    "Indonesia": "107.386571,-4.548558,119.5970811,7.3722212",
    "Kenya": "33.890677499999995,-4.6681352,41.8597969,5.503299",
    "Liberia": "-11.423899,4.3663899,-7.5426210000000005,8.533899",
    "Malawi": "32.700001,-16.766699,35.769999000000006,-9.2333343",
    "Nepal": "79.9927788,26.3721864,88.219999,30.4708008",
    "Nigeria": "2.6925510000000004,4.240001,14.5758323,13.866651000000001",
}

country_choice = st.selectbox("Choose a country:", options=countries)

viewer_url = f"https://kshitijrajsharma.github.io/overture-to-tiles/?url=https%3A%2F%2Fstaging-raw-data-api.s3.amazonaws.com%2Fdefault%2Foverture%2F2024-05-16-beta.0%2F{country_choice.lower()}%2Fpmtiles"

st.markdown(f"[Load this Area in viewer]({viewer_url})", unsafe_allow_html=True)
dataset_choice = st.selectbox("Choose a dataset:", options=datasets)

s3_parquet_url = (
    f"{base_url}/{country_choice.lower()}/parquet/{dataset_choice}.geo.parquet"
)
bbox_input = country_bboxes[country_choice]

st.text_area("Parquet URL:", s3_parquet_url)
st.text_input("Bounding Box (e.g., 'minx,miny,maxx,maxy'):", value=bbox_input)

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
                if os.getenv("Token"):
                    headers["access-token"] = os.getenv("Token")
                geom_dump = json.dumps({"geometry": geometry})

                for _ in range(max_retries):
                    try:
                        with st.spinner("Calling bbox meta stats API..."):
                            response = requests.post(
                                url, data=geom_dump, headers=headers
                            )
                        response.raise_for_status()
                        return response.json()
                    except requests.exceptions.RequestException as e:
                        st.warning(f"Error fetching data: {e}")
                        continue

                st.error("Failed to fetch data after multiple retries.")
                return None

            data = fetch_data(geometry)
            if data:
                refined_data = {
                    "population": data["raw"]["population"],
                    "populatedAreaKm2": data["raw"]["populatedAreaKm2"],
                    "osmBuildingsCount": data["raw"]["osmBuildingsCount"],
                    "osmHighwayLengthKm": data["raw"]["osmHighwayLengthKm"],
                    "buildingCount6Months": data["raw"]["buildingCount6Months"],
                    "highwayLength6MonthsKm": data["raw"]["highwayLength6MonthsKm"],
                }
                st.json(refined_data)
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

                st.success("Meta Data inserted into poly_stats table.")
                poly_stats_sql = f"""WITH t1 AS (SELECT COUNT(pg.*) AS total_parquet_rows FROM parquet_data pq),
t2 AS (SELECT ps.population, ps.osmBuildingsCount FROM poly_stats ps)
SELECT t1.total_parquet_rows, t2.population, t2.osmBuildingsCount, (t1.total_parquet_rows / t2.population) AS people_per_building FROM t1, t2;"""
                try:
                    with st.spinner("Fetching bbox meta stats..."):
                        df = con.execute(poly_stats_sql).df()
                    st.dataframe(df)
                except Exception as ex:
                    st.error(ex)

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
