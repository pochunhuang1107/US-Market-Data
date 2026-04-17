import io
import boto3
import pyarrow.parquet as pq

bucket = "us-market-data-casper"
key = "curated/reference/etf_symbol_metadata/data.parquet"

s3 = boto3.client("s3")
obj = s3.get_object(Bucket=bucket, Key=key)
table = pq.read_table(io.BytesIO(obj["Body"].read()))
df = table.to_pandas().sort_values("symbol")

print(df.to_string(index=False))