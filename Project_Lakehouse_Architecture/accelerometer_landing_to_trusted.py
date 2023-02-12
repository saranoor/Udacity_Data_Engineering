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

# Script generated for node Customer Trusted
CustomerTrusted_node1676205835323 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1676205835323",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1676205835323,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1676206337795 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1676206337795",
)

# Script generated for node Accelerated Trusted
AcceleratedTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676206337795,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AcceleratedTrusted_node3",
)

job.commit()
