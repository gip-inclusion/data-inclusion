AWS_CONN_ID = "s3"
BUCKET = "data-inclusion-lake"
LIMIT = 20
def main() -> None:
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = hook.get_client_type("s3")
    response = client.list_objects_v2(Bucket=BUCKET)
    print(f"bucket={BUCKET}")
    for obj in response.get("Contents", [])[:LIMIT]:
        print(obj["Key"])
if __name__ == "__main__":

    main()
