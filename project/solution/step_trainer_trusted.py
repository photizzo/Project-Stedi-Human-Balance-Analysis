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

# Script generated for node S3 Trusted Accelerator
S3TrustedAccelerator_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="S3TrustedAccelerator_node1",
)

# Script generated for node S3 Landing Step Trainer
S3LandingStepTrainer_node1690308773573 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="S3LandingStepTrainer_node1690308773573",
)

# Script generated for node Join
Join_node1690309104474 = Join.apply(
    frame1=S3TrustedAccelerator_node1,
    frame2=S3LandingStepTrainer_node1690308773573,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1690309104474",
)

# Script generated for node Drop Fields
DropFields_node1690310741537 = DropFields.apply(
    frame=Join_node1690309104474,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1690310741537",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1690310741537,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-ng/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
