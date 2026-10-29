import json
from collections.abc import Iterator
from getpass import getpass
from pathlib import Path

import psycopg
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from psycopg import sql


PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent

LAYER_TABLES = {
    "towns": "towns",
    "constituencies": "constituencies",
    "country_boundary": "country_boundary",
    "regions": "regions",
    "mineral_occurrences": "mineral_occurrences",
    "geological_units": "geological_units",
    "faults": "faults",
    "dykes_250k": "dykes_250k",
    "etendeka_dykes": "etendeka_dykes",
    "karoo_dykes": "karoo_dykes",
    "roads": "roads",
    "contours": "contours",
}

app = FastAPI(title="Geosynth Namibia")


# Open a database connection with read-only transactions.
def connect_database() -> psycopg.Connection:
    return psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="geosynth_demo",
        user="postgres",
        password=app.state.database_password,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    )


# Redirect the root address to the map.
@app.get("/")
def open_map() -> RedirectResponse:
    return RedirectResponse(url="/frontend/")


# Check the database connection and PostGIS version.
@app.get("/api/health")
def get_health() -> dict:
    with connect_database() as connection:
        result = connection.execute(
            "SELECT PostGIS_Version()"
        ).fetchone()

    return {
        "status": "connected",
        "database": "geosynth_demo",
        "postgis_version": result[0],
    }


# Stream full GeoJSON records without aggregating the entire layer.
def stream_layer(layer_name: str) -> Iterator[bytes]:
    table_identifier = sql.Identifier(
        "geosynth",
        LAYER_TABLES[layer_name],
    )

    query = sql.SQL("""
        SELECT json_build_object(
            'type', 'Feature',
            'id', record_number,
            'properties', properties,
            'geometry', ST_AsGeoJSON(geometry, 15, 0)::json
        )::text
        FROM {}
        ORDER BY record_number
    """).format(table_identifier)

    with connect_database() as connection:
        with connection.cursor(name="map_layer_cursor") as cursor:
            cursor.itersize = 50
            cursor.execute(query)

            header = (
                '{"type":"FeatureCollection","name":'
                + json.dumps(layer_name)
                + ',"features":['
            )

            yield header.encode("utf-8")

            first_feature = True

            for feature_record in cursor:
                prefix = b"" if first_feature else b","
                yield prefix + feature_record[0].encode("utf-8")
                first_feature = False

            yield b"]}"


# Return a configured layer as one streamed GeoJSON document.
@app.get("/api/layers/{layer_name}")
def get_layer(layer_name: str) -> StreamingResponse:
    if layer_name not in LAYER_TABLES:
        raise HTTPException(
            status_code=404,
            detail="Unknown map layer.",
        )

    return StreamingResponse(
        stream_layer(layer_name),
        media_type="application/json",
    )


# Find mineral occurrences within a straight-line distance of a town.
@app.get("/api/search/near-town")
def search_near_town(
    town: str,
    distance_km: float = 50,
) -> dict:
    town = town.strip()

    if not town:
        raise HTTPException(
            status_code=400,
            detail="Enter a town name.",
        )

    if not 0 < distance_km <= 500:
        raise HTTPException(
            status_code=400,
            detail="Distance must be greater than 0 and at most 500 km.",
        )

    with connect_database() as connection:
        town_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM geosynth.towns
            WHERE LOWER(properties->>'NAME') = LOWER(%s)
            """,
            (town,),
        ).fetchone()[0]

        if town_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Town not found.",
            )

        if town_count != 1:
            raise HTTPException(
                status_code=400,
                detail="Town name matches more than one record.",
            )

        result = connection.execute(
            """
            WITH selected_town AS (
                SELECT geometry
                FROM geosynth.towns
                WHERE LOWER(properties->>'NAME') = LOWER(%s)
            ),
            matches AS (
                SELECT
                    mineral.record_number,
                    mineral.properties,
                    mineral.geometry,
                    ST_Distance(
                        mineral.geometry::geography,
                        selected_town.geometry::geography
                    ) / 1000 AS distance_km
                FROM geosynth.mineral_occurrences AS mineral
                CROSS JOIN selected_town
                WHERE ST_DWithin(
                    mineral.geometry::geography,
                    selected_town.geometry::geography,
                    %s
                )
            )
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'type', 'Feature',
                            'id', record_number,
                            'properties',
                                properties || jsonb_build_object(
                                    'distance_km', distance_km
                                ),
                            'geometry',
                                ST_AsGeoJSON(geometry, 15, 0)::jsonb
                        )
                        ORDER BY distance_km, record_number
                    ),
                    '[]'::jsonb
                )
            )
            FROM matches
            """,
            (town, distance_km * 1000),
        ).fetchone()

    return result[0]


app.mount(
    "/frontend",
    StaticFiles(
        directory=PROJECT_DIRECTORY / "frontend",
        html=True,
    ),
    name="frontend",
)


# Verify credentials and start the local server using Winloop.
def main():
    app.state.database_password = getpass("PostgreSQL password: ")

    with connect_database() as connection:
        connection.execute("SELECT 1")

    print("Database connection verified.", flush=True)
    print("Starting web server...", flush=True)

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8001,
        loop="winloop:new_event_loop",
        log_level="info",
    )


if __name__ == "__main__":
    main()