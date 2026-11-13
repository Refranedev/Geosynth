import json
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg.types.json import Jsonb


PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent
DATA_DIRECTORY = PROJECT_DIRECTORY / "data" / "processed"

# Use the same database as app.py.
load_dotenv(PROJECT_DIRECTORY / ".env")
DATABASE_URL = os.getenv("DATABASE_URL")

# Batches are limited by rows AND by geometry size, so huge polygons
# are sent alone instead of in a pile the server cannot digest.
MAX_ROWS_PER_BATCH = 25
MAX_BYTES_PER_BATCH = 250_000
MAX_RETRIES = 5

# 6 decimal places is about 0.1 m: plenty for a map, much smaller to send.
COORDINATE_DECIMALS = 6

DATASETS = {
    "towns": "base_admin/Major Towns.geojson",
    "constituencies": "base_admin/Namibia Constituencies.geojson",
    "country_boundary": "base_admin/Namibia Country Border.geojson",
    "regions": "base_admin/Namibian Regions.geojson",
    "mineral_occurrences": "commodities/Known Mineral Occurences.geojson",
    "geological_units": "geology/Lithostratigraphic Units.geojson",
    "faults": "geology/Structures/Faults.geojson",
    "dykes_250k": "geology/Structures/Dykes_250k.geojson",
    "etendeka_dykes": "geology/Structures/Etendeka Dykes.geojson",
    "karoo_dykes": "geology/Structures/Karoo Dykes.geojson",
    "roads": "infrastructure/Main Roads.geojson",
    "contours": (
        "geology/Elevation/"
        "Southern African Contours_1km_res_500m intervals line.geojson"
    ),
}


# Open a fresh connection that stays alive during long uploads.
def open_connection():
    connection = psycopg.connect(
        DATABASE_URL,
        connect_timeout=15,
        autocommit=True,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    connection.execute("SET statement_timeout = 0")
    return connection


# Round every coordinate in a nested coordinate list.
def round_coordinates(value):
    if isinstance(value, float):
        return round(value, COORDINATE_DECIMALS)
    if isinstance(value, list):
        return [round_coordinates(item) for item in value]
    return value


# Round a geometry (and any child geometries) in place.
def round_geometry(geometry):
    if geometry is None:
        return
    if geometry["type"] == "GeometryCollection":
        for child in geometry["geometries"]:
            round_geometry(child)
    else:
        geometry["coordinates"] = round_coordinates(geometry["coordinates"])


# Build one insert row and report the size of its geometry.
def build_row(record_number, feature):
    geometry = feature.get("geometry")
    round_geometry(geometry)

    geometry_text = (
        json.dumps(geometry, allow_nan=False, separators=(",", ":"))
        if geometry is not None else None
    )

    row = (
        record_number,
        Jsonb(feature.get("properties")),
        Jsonb(feature.get("id")),
        geometry_text,
    )
    return row, len(geometry_text) if geometry_text else 0


# Group missing records into batches limited by rows and by size.
def make_batches(features, missing_numbers):
    batch, batch_bytes = [], 0

    for record_number in missing_numbers:
        row, row_bytes = build_row(record_number, features[record_number - 1])

        too_big = batch and batch_bytes + row_bytes > MAX_BYTES_PER_BATCH
        if batch and (len(batch) >= MAX_ROWS_PER_BATCH or too_big):
            yield batch, batch_bytes
            batch, batch_bytes = [], 0

        batch.append(row)
        batch_bytes += row_bytes

    if batch:
        yield batch, batch_bytes


# Import one dataset, resuming from whatever is already saved.
def import_dataset(table_name, relative_path):
    source_path = DATA_DIRECTORY / relative_path

    with source_path.open(encoding="utf-8") as source_file:
        dataset = json.load(source_file)

    if dataset.get("type") != "FeatureCollection":
        raise ValueError(f"Invalid FeatureCollection: {relative_path}")

    features = dataset["features"]
    total = len(features)
    table_identifier = sql.Identifier("geosynth", table_name)
    index_identifier = sql.Identifier(f"{table_name}_geometry_index")
    repaired_count = 0

    connection = open_connection()

    try:
        connection.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    record_number INTEGER PRIMARY KEY,
                    properties JSONB,
                    source_feature_id JSONB,
                    geometry geometry,
                    CHECK (
                        geometry IS NULL OR ST_SRID(geometry) = 4326
                    )
                )
            """).format(table_identifier)
        )

        saved_numbers = {
            row[0] for row in connection.execute(
                sql.SQL("SELECT record_number FROM {}").format(table_identifier)
            )
        }

        # More rows than the file has means stale data: start clean.
        if len(saved_numbers) > total:
            connection.execute(
                sql.SQL("DELETE FROM {}").format(table_identifier)
            )
            saved_numbers = set()

        missing_numbers = [
            number for number in range(1, total + 1)
            if number not in saved_numbers
        ]

        if not missing_numbers:
            print(f"Skipping {table_name}: already has {total} records.",
                  flush=True)
        else:
            print(
                f"Importing {table_name}: {len(missing_numbers)} of "
                f"{total} records to go...",
                flush=True,
            )

            insert_statement = sql.SQL("""
                INSERT INTO {} (
                    record_number,
                    properties,
                    source_feature_id,
                    geometry
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    ST_SetSRID(ST_GeomFromGeoJSON(%s::text), 4326)
                )
                ON CONFLICT (record_number) DO NOTHING
            """).format(table_identifier)

            saved_in_run = 0
            batch_number = 0

            for rows, batch_bytes in make_batches(features, missing_numbers):
                batch_number += 1
                first, last = rows[0][0], rows[-1][0]

                # Announce heavy batches BEFORE sending, so a hang is visible.
                if batch_bytes > 1_000_000:
                    print(
                        f"  sending records {first}-{last} "
                        f"({batch_bytes / 1_000_000:.1f} MB)...",
                        flush=True,
                    )

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        with connection.transaction():
                            with connection.cursor() as cursor:
                                cursor.executemany(insert_statement, rows)
                        break
                    except psycopg.OperationalError as error:
                        print(
                            f"  Connection problem at records {first}-{last} "
                            f"({batch_bytes / 1_000_000:.1f} MB, attempt "
                            f"{attempt}/{MAX_RETRIES}): "
                            f"{str(error).splitlines()[0]}",
                            flush=True,
                        )
                        try:
                            connection.close()
                        except Exception:
                            pass
                        if attempt == MAX_RETRIES:
                            raise
                        time.sleep(3 * attempt)
                        connection = open_connection()

                saved_in_run += len(rows)
                if batch_number % 10 == 0 or saved_in_run == len(missing_numbers):
                    print(
                        f"  {len(saved_numbers) + saved_in_run}/{total}",
                        flush=True,
                    )

        # Repair constituency geometry (safe to run more than once).
        if table_name == "constituencies":
            connection.execute(
                sql.SQL("""
                    ALTER TABLE {}
                    ADD COLUMN IF NOT EXISTS original_geometry geometry
                """).format(table_identifier)
            )

            cursor = connection.execute(
                sql.SQL("""
                    UPDATE {}
                    SET
                        original_geometry = geometry,
                        geometry = ST_MakeValid(geometry)
                    WHERE geometry IS NOT NULL
                      AND NOT ST_IsValid(geometry)
                """).format(table_identifier)
            )
            repaired_count = cursor.rowcount

            unusable = connection.execute(
                sql.SQL("""
                    SELECT COUNT(*)
                    FROM {}
                    WHERE original_geometry IS NOT NULL
                      AND (
                          geometry IS NULL
                          OR ST_IsEmpty(geometry)
                          OR NOT ST_IsValid(geometry)
                          OR GeometryType(geometry) NOT IN (
                              'POLYGON', 'MULTIPOLYGON'
                          )
                      )
                """).format(table_identifier)
            ).fetchone()[0]

            if unusable:
                raise ValueError(
                    "Constituency repair produced an unusable boundary."
                )

        connection.execute(
            sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {} USING GIST (geometry)
            """).format(index_identifier, table_identifier)
        )

        imported_count, missing_geometry, invalid_geometry = connection.execute(
            sql.SQL("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE geometry IS NULL),
                    COUNT(*) FILTER (
                        WHERE geometry IS NOT NULL
                          AND NOT ST_IsValid(geometry)
                    )
                FROM {}
            """).format(table_identifier)
        ).fetchone()

        if imported_count != total:
            raise ValueError(
                f"Record count mismatch for {table_name}: "
                f"{imported_count} saved, {total} expected."
            )

        connection.execute(sql.SQL("ANALYZE {}").format(table_identifier))

        print(
            f"Saved {table_name}: {imported_count} records; "
            f"{missing_geometry} missing geometries; "
            f"{invalid_geometry} invalid geometries; "
            f"{repaired_count} repaired geometries.",
            flush=True,
        )
    finally:
        try:
            connection.close()
        except Exception:
            pass


# Import all configured datasets into the same database as app.py.
def main():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set in the .env file.")

    for relative_path in DATASETS.values():
        source_path = DATA_DIRECTORY / relative_path

        if not source_path.is_file():
            raise FileNotFoundError(source_path)

    with open_connection() as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        connection.execute("CREATE SCHEMA IF NOT EXISTS geosynth")

    for table_name, relative_path in DATASETS.items():
        import_dataset(table_name, relative_path)

    print(f"\nFinished: all {len(DATASETS)} datasets imported.")


if __name__ == "__main__":
    main()