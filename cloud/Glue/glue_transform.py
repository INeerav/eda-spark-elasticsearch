import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702998018319 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://music-951f64e0/csv/artists.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702998018319",
)

# Script generated for node Change Schema
ChangeSchema_node1702998087670 = ApplyMapping.apply(
    frame=AmazonS3_node1702998018319,
    mappings=[
        ("mbid", "string", "mbid", "string"),
        ("artist_mb", "string", "artist_mb", "string"),
        ("artist_lastfm", "string", "artist_lastfm", "string"),
        ("country_mb", "string", "country_mb", "string"),
        ("country_lastfm", "string", "country_lastfm", "string"),
        ("tags_mb", "string", "tags_mb", "string"),
        ("tags_lastfm", "string", "tags_lastfm", "string"),
        ("listeners_lastfm", "string", "listeners_lastfm", "string"),
        ("scrobbles_lastfm", "string", "scrobbles_lastfm", "string"),
        ("ambiguous_artist", "string", "ambiguous_artist", "boolean"),
    ],
    transformation_ctx="ChangeSchema_node1702998087670",
)

# Script generated for node Amazon S3
AmazonS3_node1702998138936 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1702998087670,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://music-951f64e0/parquet/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1702998138936",
)

job.commit()
