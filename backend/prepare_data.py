import json
import math
from pathlib import Path

from pyproj import Transformer


PROJECT_DIRECTORY = Path(__file__).resolve().parent.parent
RAW_DIRECTORY = PROJECT_DIRECTORY / "data" / "raw"
PROCESSED_DIRECTORY = PROJECT_DIRECTORY / "data" / "processed"

COORDINATE_SYSTEMS = {
    "urn:ogc:def:crs:EPSG::32733": "EPSG:32733",
    "urn:ogc:def:crs:ESRI::102022": "ESRI:102022",
    "urn:ogc:def:crs:OGC:1.3:CRS84": "OGC:CRS84",
}


# Convert nested geometry coordinates to longitude and latitude.
def convert_coordinates(coordinates, transformer) -> list:
    if not coordinates:
        return []

    if isinstance(coordinates[0], (int, float)):
        longitude, latitude = transformer.transform(
            coordinates[0],
            coordinates[1],
            errcheck=True,
        )

        if not (
            math.isfinite(longitude)
            and math.isfinite(latitude)
            and -180 <= longitude <= 180
            and -90 <= latitude <= 90
        ):
            raise ValueError("Invalid transformed coordinates.")

        return [longitude, latitude, *coordinates[2:]]

    return [
        convert_coordinates(component, transformer)
        for component in coordinates
    ]


# Convert a geometry and remove any outdated bounding box.
def convert_geometry(geometry, transformer):
    if geometry is None:
        return

    geometry.pop("bbox", None)

    if geometry["type"] == "GeometryCollection":
        for child_geometry in geometry["geometries"]:
            convert_geometry(child_geometry, transformer)
    else:
        geometry["coordinates"] = convert_coordinates(
            geometry["coordinates"],
            transformer,
        )


# Prepare one GeoJSON file while preserving its attributes.
def prepare_file(source_path):
    with source_path.open(encoding="utf-8-sig") as source_file:
        dataset = json.load(source_file)

    if dataset.get("type") != "FeatureCollection":
        raise ValueError(f"Expected a FeatureCollection: {source_path.name}")

    declared_system = dataset.get("crs", {}).get(
        "properties", {}
    ).get("name")

    if declared_system not in COORDINATE_SYSTEMS:
        raise ValueError(
            f"Unknown coordinate system in {source_path.name}: "
            f"{declared_system}"
        )

    transformer = Transformer.from_crs(
        COORDINATE_SYSTEMS[declared_system],
        "OGC:CRS84",
        always_xy=True,
    )

    for feature in dataset["features"]:
        feature.pop("bbox", None)
        convert_geometry(feature.get("geometry"), transformer)

    # Output coordinates now use GeoJSON longitude/latitude order.
    dataset.pop("crs", None)
    dataset.pop("bbox", None)

    relative_path = source_path.relative_to(RAW_DIRECTORY)
    output_path = PROCESSED_DIRECTORY / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(
            dataset,
            output_file,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )

    print(f"Prepared: {relative_path} ({len(dataset['features'])} records)")


# Prepare every GeoJSON, including files inside subfolders.
def main():
    source_paths = sorted(RAW_DIRECTORY.rglob("*.geojson"))

    if not source_paths:
        raise FileNotFoundError("No GeoJSON files found in data/raw.")

    for source_path in source_paths:
        prepare_file(source_path)

    print(f"\nFinished: {len(source_paths)} map datasets prepared.")


if __name__ == "__main__":
    main()