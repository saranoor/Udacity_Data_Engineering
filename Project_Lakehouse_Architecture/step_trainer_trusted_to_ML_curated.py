import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1676295028951 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1676295028951",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1676266279502 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1676266279502",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1676272392002 = DynamicFrame.fromDF(
    CustomerCurated_node1676266279502.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicates_node1676272392002",
)

# Script generated for node Rename SerialNumber in customer curated
RenameSerialNumberincustomercurated_node1676272914316 = RenameField.apply(
    frame=DropDuplicates_node1676272392002,
    old_name="serialNumber",
    new_name="serialnumber",
    transformation_ctx="RenameSerialNumberincustomercurated_node1676272914316",
)

# Script generated for node FIltering Non curated
FIlteringNoncurated_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenameSerialNumberincustomercurated_node1676272914316,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="FIlteringNoncurated_node2",
)

# Script generated for node Drop Fields
DropFields_node1676272690405 = DropFields.apply(
    frame=FIlteringNoncurated_node2,
    paths=[
        "timeStamp",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
        "phone",
        "serialnumber",
    ],
    transformation_ctx="DropFields_node1676272690405",
)

# Script generated for node Join
Join_node1676295127298 = Join.apply(
    frame1=DropFields_node1676272690405,
    frame2=AccelerometerTrusted_node1676295028951,
    keys1=["email", "sensorReadingTime"],
    keys2=["user", "timeStamp"],
    transformation_ctx="Join_node1676295127298",
)

# Script generated for node Amazon S3
AmazonS3_node1676295316333 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1676295127298,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-lake-house/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1676295316333",
)

job.commit()
