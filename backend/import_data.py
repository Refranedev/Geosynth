import json
from getpass import getpass
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent
DATA_DIRECTORY = PROJECT_DIRECTORY / "data" / "processed"

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


# Import one dataset and verify its record count.
def import_dataset(connection, table_name, relative_path):
    source_path = DATA_DIRECTORY / relative_path

    with source_path.open(encoding="utf-8") as source_file:
        dataset = json.load(source_file)

    if dataset.get("type") != "FeatureCollection":
        raise ValueError(f"Invalid FeatureCollection: {relative_path}")

    features = dataset["features"]
    table_identifier = sql.Identifier("geosynth", table_name)
    index_identifier = sql.Identifier(f"{table_name}_geometry_index")
    repaired_count = 0

    print(f"Importing {table_name}: {len(features)} records...", flush=True)

    # Replace each dataset atomically so a failure rolls back its changes.
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
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

            cursor.execute(
                sql.SQL("DELETE FROM {}").format(table_identifier)
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
            """).format(table_identifier)

            rows = (
                (
                    record_number,
                    Jsonb(feature.get("properties")),
                    Jsonb(feature.get("id")),
                    (
                        json.dumps(
                            feature["geometry"],
                            allow_nan=False,
                            separators=(",", ":"),
                        )
                        if feature.get("geometry") is not None
                        else None
                    ),
                )
                for record_number, feature in enumerate(features, start=1)
            )

            cursor.executemany(insert_statement, rows)

            # Preserve original constituency geometry before repairing it.
            if table_name == "constituencies":
                cursor.execute(
                    sql.SQL("""
                        ALTER TABLE {}
                        ADD COLUMN IF NOT EXISTS original_geometry geometry
                    """).format(table_identifier)
                )

                cursor.execute(
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

                # Reject unusable repaired boundaries and roll back the import.
                cursor.execute(
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
                )

                if cursor.fetchone()[0]:
                    raise ValueError(
                        "Constituency repair produced an unusable boundary."
                    )

            cursor.execute(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {}
                    ON {} USING GIST (geometry)
                """).format(index_identifier, table_identifier)
            )

            cursor.execute(
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
            )

            imported_count, missing_geometry, invalid_geometry = (
                cursor.fetchone()
            )

            if imported_count != len(features):
                raise ValueError(f"Record count mismatch: {table_name}")

            cursor.execute(
                sql.SQL("ANALYZE {}").format(table_identifier)
            )

    print(
        f"Saved {table_name}: {imported_count} records; "
        f"{missing_geometry} missing geometries; "
        f"{invalid_geometry} invalid geometries; "
        f"{repaired_count} repaired geometries.",
        flush=True,
    )


# Connect to the project database and import all configured datasets.
def main():
    for relative_path in DATASETS.values():
        source_path = DATA_DIRECTORY / relative_path

        if not source_path.is_file():
            raise FileNotFoundError(source_path)

    password = getpass("PostgreSQL password: ")

    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="geosynth_demo",
        user="postgres",
        password=password,
        autocommit=True,
    ) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS geosynth")

        for table_name, relative_path in DATASETS.items():
            import_dataset(connection, table_name, relative_path)

    print(f"\nFinished: all {len(DATASETS)} datasets imported into PostgreSQL.")


if __name__ == "__main__":
    main()