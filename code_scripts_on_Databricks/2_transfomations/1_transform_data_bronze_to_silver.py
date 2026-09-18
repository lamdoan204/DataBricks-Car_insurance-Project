from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap, split, size, when, concat, lit, from_json, 
    abs, to_timestamp, regexp_extract, year, current_date, regexp_replace, struct, round, substring_index
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, 
)
import random
catalog = "car_insurance_project"
bronze_schema = "1_bronze"
silver_schema = '2_silver'



# customer table
@dlt.table(
    name='cleaned_customers'
)
@dlt.expect_all_or_fail({
    "valid_first_name": "first_name is not null and first_name = regexp_replace(trim(first_name), ' {2,}', ' ')",
    "valid_last_name" : "last_name is not null and last_name = regexp_replace(trim(last_name), ' {2,}', ' ')",
    "valid_date_of_birth": "date_of_birth is not null ",
    "valid_address": "address is not null and address = trim(address)",
    "valid_zip_code": "zip_code is not null and "
    })
def transform_customers_table():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.customers")
    df = df.withColumn(
        "date_of_birth_parsed", to_date(col("date_of_birth"), "dd-MM-yyyy")
    )
    df = df.withColumn(
        "age", year(current_date()) - year(col("date_of_birth_parsed"))
    )
    df = df.withColumn("neighborhood", when(
               col('neighborhood').isNotNull(), trim(col("neighborhood"))
           ).otherwise(
               "neighborhood not available"
           )).withColumn("borough", when(
               col('borough').isNull(), 'borough not available'
           ).otherwise(
               col('borough')
           ) )

    return(
    df.withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn(
        "first_name",
        when(col('name').isNull(), 'Not available')
        .otherwise(
            when(size(split(col('name'), ',')) == 2, trim(split(col('name'), ',')[1]))
            .otherwise(trim(split(col('name'), ',')[-1]))
        )
    )
    .withColumn(
        "last_name",
        when(col('name').isNull(), 'Not available')
        .otherwise(
            when(size(split(col('name'), ',')) == 2, trim(split(col('name'), ',')[0]))
            .otherwise(trim(split(col('name'), ',')[0]))
        )
    )
    .withColumn(
        "date_of_birth",
        when((col("age") >= 18) & (col("age") <= 120),
             to_date(col('date_of_birth'), 'dd-MM-yyyy')
        ).otherwise(to_date(lit('01-01-1990'), 'dd-MM-yyyy'))
    )
    .withColumn(
        "address",
        concat(col("neighborhood"), lit(', '), col("borough"))
    )
    .withColumn(
        "zip_code",
        when(col("zip_code").isNotNull(), col("zip_code").cast("int"))
        .otherwise(-1)
    )
    .dropna()
    .dropDuplicates(['customer_id'])
    .drop("name", "_rescued_data", "age", 'neighborhood', 'borough', 'date_of_birth_parsed')
)

# policies table
@dlt.table(
    name='cleaned_policies'
)
@dlt.expect_all_or_fail({
    "valid_customer_id" : "cust_id is not null ",
    "valid_policy_type": "policy_type is not null ",
    "valid_pol_issue_date" : "pol_eff_date < pol_expiry_date",
    "valid_make" : "make is not null and make = trim(make)",
    "valid_model" : "model is not null and model = trim(model)",
    "valid_model_year" : "model_year is not null",
    "valid_chassis_no" : "chassis_no = trim(chassis_no) and chassis_no not like '%.%'",
    "valid_use_of_vehicle": "use_of_vehicle is not null",
    "valid_product" : "product is not null and trim(product) = product",
    "valid_sum_insured" : "sum_insured > 0 and sum_insured > premium",
    "valid_premium" : "premium >= 0 and premium is not null",
    "valid_dedcutable" : "deductable > 0 and deductable is not null"
})
def transform_policies_table():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.policies")
    return (
        df.withColumn('policy_no', trim(col('policy_no')))
        .withColumn("cust_id", col("cust_id").cast('int'))
        .withColumn('policy_type', when(col('policy_type') == "TP", 'Third Party' ).otherwise(
            'Comprehensive'
        ))
        .withColumn('pol_issue_date', to_date(col('pol_issue_date'), 'dd-MM-yyy'))
        .withColumn('pol_expiry_date', to_date(col('pol_expiry_date'), 'dd-MM-yyy'))
        .withColumn('pol_eff_date', to_date(col('pol_eff_date'), 'dd-MM-yyy'))
        .withColumn('make', when(
            (col('make').isNull()) | (col('make') == '.'), 'Not available' 
        ).otherwise(trim(col('make'))))
        .withColumn("model", when(
            col('model').isNull(), 'Not available'
        ).otherwise(
            trim(col('model'))
        ))
        .withColumn('model_year', when(
            col('model_year').isNull(), -1
        ).otherwise(abs(col('model_year'))).cast('int'))
        .withColumn('chassis_no', when(
            col('chassis_no').isNull(), 'Not available'
        ).otherwise(
            trim(regexp_replace(col('chassis_no'), '\\.', ''))
        ))
        .withColumn('use_of_vehicle', when(
            col('use_of_vehicle').isNull(), 'Not available'
        ). otherwise(
            when(
                col('use_of_vehicle') == 'RENTACAR', 'RENT A CAR'
            ).otherwise(
                when(
                    col('use_of_vehicle').like('%/%'), trim(regexp_replace(col('use_of_vehicle'), '/', ' OR')) 
                ).otherwise(
                    trim(col('use_of_vehicle'))
                )
            )
        ))
        .withColumn('product', when(
            col('product').isNull(), 'Not available'
        ).otherwise(
            when(
                col('product') == 'TP', 'Third Party'
            ).otherwise(
                trim(col('product'))
            )
        ))
        .withColumn('sum_insured', abs(col('sum_insured'))
        )
        .withColumn('premium', abs(col('premium')))
        .withColumn('deductable', abs(col('deductable')))
        .dropna(subset=['chassis_no']).dropDuplicates(['policy_no']).filter("sum_insured > premium")
    )


# insurance_claims table
@dlt.table(
    name='cleaned_insurance_claims'
)
@dlt.expect_all_or_fail({
    "valid_claim_no" : "claim_no is not null and claim_no = trim(claim_no)",
    "valid_policy_no": "policy_no is not null and policy_no = trim(policy_no)",
    "months_as_customer" : "months_as_customer >= 0",
    "valid_money" : "injury >= 0 and property >= 0 and vehicle >= 0 and total >= 0 and total = injury + property + vehicle",
    "valid_collision_type" : " collision_type is not null and collision_type = trim(collision_type)",
    "valid_number_of_vehcles_involed": "number_of_vehicles_involved >= 0",
    "valid_age" : "age > 0 and typeof(age) = 'int'",
    "valid_insured_relationship": "insured_relationship = trim(insured_relationship)",
    "valid_accidnent_type" : "accident_type = trim(accident_type)",
    "valid_serverity" : "severity = trim(severity)",
    "valid_number_of_witnesses" : "number_of_witnesses >= 0"
})
def transform_insurance_claims_table():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.insurance_claims")
    return(
        df.withColumn(
            "claim_no", 
            trim(col('claim_no'))
        ).withColumn(
            'claim_date',
            to_date(col('claim_date'), 'dd-MM-yyyy')
        ).withColumn(
            'months_as_customer',
            abs(col('months_as_customer'))
        ).withColumn(
            'injury',
            abs(col('injury'))
        ).withColumn(
            'property',
            abs(col('property'))
        ).withColumn(
            'vehicle',
            abs(col('vehicle'))
        ).withColumn(
            'total',
            when(
                (col('total').isNull()) | (col('total') != col('injury') + col('property') + col('vehicle')),
                col('injury') + col('property') + col('vehicle')
            ).otherwise(
                col('total')
            )
        ).withColumn(
            'collision_type', 
            when(
                col('collision_type').isNull() | 
                (trim(col('collision_type')) == '') | 
                (col('collision_type') == 'null') | 
                (col('collision_type') == 'None'), 
                'Not available'
            ).otherwise(trim(col('collision_type')))
        ).withColumn(
            'number_of_vehicles_involved',
           abs(col('number_of_vehicles_involved'))
        ).withColumn(
            'age',
            abs(col('age')).cast('int')
        ).withColumn(
            'insured_relationship',
            when(
                col('insured_relationship').isNull(), 'Not available'
                ).otherwise(
                    trim(col('insured_relationship'))
                )
        ).withColumn(
            'license_issue_date',
            to_date(col('license_issue_date'), 'dd-MM-yyyy')
        ).withColumn(
            'accident_date',
            to_date(col('accident_date'), 'dd-MM-yyy')
        ).withColumn(
            'accident_hour',
            when(
               (col('accident_hour') < 0) | (col('accident_hour') > 23), random.randint(0, 24)
            ).otherwise(
                abs(col('accident_hour'))
            )
        ).withColumn(
            'accident_type',
            when(
                col('accident_type').isNull(), 'Not available'
            ).otherwise(
                trim(col('accident_type'))
            )
        ).withColumn(
            'severity',
            when(
                col('severity').isNull(), 'Not available'
            ).otherwise(
                trim(col('severity'))
            )
        ).withColumn(
            'number_of_witnesses',
            when(
                col('number_of_witnesses').isNull(), 0
            ).otherwise(abs(col('number_of_witnesses')))
        ).withColumn(
            'suspicious_activity', col("suspicious_activity")
        )
        ).dropDuplicates(['claim_no']).dropna()

# telematics table
@dlt.table(
    name = "cleaned_telematics"
)
@dlt.expect_all_or_fail({
    'valid_topic' : 'topic is not null',
    
})
def transform_telematics_table():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.telemactics")

    payload_schema = StructType([
        StructField("chassis_no", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
    ])
    parsed_stream = df.selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "partition",
        "topic",
        "key",
        "timestamp"
    ).withColumn(
        "decoded_data",
        from_json(col("raw_json"), payload_schema)
    ).withColumn(
        "metadata",
        struct(
            col("partition"),
            col("topic"),
        )
    )
    return(
        parsed_stream.select(
            col("decoded_data.chassis_no"),
            col("decoded_data.latitude"),
            col("decoded_data.longitude"),
            round(col("decoded_data.speed"), 3).alias('speed'),
            col("timestamp"),
            col("metadata")
        )
    )

# claim_images_metadata table
@dlt.table(
    name ='cleaned_claim_images_metadata'
)
@dlt.expect_all_or_fail({
    "valid_image_name": "image_name is not null",
    'valid_image_id': "image_id is not null and typeof(image_id) = 'int' ",
    'valid_claim_no': 'claim_no is not null',
    'valid_chassis_no': 'chassis_no is not null'
    })
def transfrom_claim_images_metadata_table():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images_metadata")
    return(
        df.withColumn("image_name", when(col("image_name").isNull(), "Not available")
                                  .otherwise(col("image_name")))
        .withColumn("image_id", when(col("image_id").isNull(), -1)
                                .otherwise(col('image_id')).cast("int"))
        .withColumn("claim_no", when(col("claim_no").isNull(), 'Not available')
                                .otherwise(col('claim_no')))
        .withColumn("chassis_no", when(col("chassis_no").isNull(), 'Not available')
                                .otherwise(col('chassis_no')))
        .drop("_rescued_data")
    )

# trainning images 
@dlt.table(
    name = "cleand_training_images"
)
def transform_traiing_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images")
    
    return df.withColumn(
        'label',
        split(
            substring_index(
                substring_index(col('path'), '/', -1 ),
                '-', -1
            ), ' '
        )[0]
    )

# claim images

@dlt.table(
    name = 'cleaned_claim_images'
)
def transform_claim_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images")
    
    return df

