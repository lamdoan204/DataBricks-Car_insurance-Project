import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf, avg
from typing import Iterator
import random

catalog = 'car_insurance_project'
silver_schema = '2_silver'
gold_schema = '3_gold'
   
@dlt.table(
    name=f"{catalog}.{gold_schema}.aggregated_telematics",
    comment="Average telematics",
    table_properties={
        "quality": "gold"
    }
)
def telematics():
    return (
        dlt.read(f"{catalog}.{silver_schema}.cleaned_telematics")
        .groupBy("chassis_no")
        .agg(
            avg("speed").alias("telematics_speed"),
            avg("latitude").alias("telematics_latitude"),
            avg("longitude").alias("telematics_longitude"),
        )
    )

@dlt.table(
    name=f"{catalog}.{gold_schema}.customer_claim_policy",
    comment = "Curated claim joined with policy records",
    table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy():
    # Read the cleaned policy records
    policy = dlt.readStream(f"{catalog}.{silver_schema}.cleaned_policies")
    # Read the cleaned claim records
    claim = dlt.readStream(f"{catalog}.{silver_schema}.cleaned_insurance_claims")
    # Read the cleaned customer records
    customer = dlt.readStream(f"{catalog}.{silver_schema}.cleaned_customers") 
    claim_policy = claim.join(policy, "policy_no")
    return claim_policy.join(customer, claim_policy.cust_id == customer.customer_id)

