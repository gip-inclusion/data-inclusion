from data_inclusion.pipeline.dags.publish_opendata.helpers import Format

DATAGOUV_DI_DATASET_ID = "6233723c2c1e4a54af2f6b2d"
DATAGOUV_DI_RESOURCE_IDS = {
    "structures": {
        Format.CSV: "fd4cb3ef-5c31-4c99-92fe-2cd8016c0ca5",
        Format.GEOJSON: "42d46a21-eeef-433c-b3c3-961e1c37bc93",
        Format.JSON: "4fc64287-e869-4550-8fb9-b1e0b7809ffa",
        Format.PARQUET: "",
        Format.XLSX: "fad88958-c9a7-4914-a9b8-89d1285c210a",
    },
    "services": {
        Format.CSV: "5abc151a-5729-4055-b0a9-d5691276f461",
        Format.GEOJSON: "307529c0-dcc5-449a-a88d-9290a8a86a14",
        Format.JSON: "0eac1faa-66f9-4e49-8fb3-f0721027d89f",
        Format.PARQUET: "",
        Format.XLSX: "de2eb57b-113d-48eb-95d2-59a69ba36eb1",
    },
}
