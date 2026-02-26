from datetime import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime, timedelta

DBUSER_PROFILE = "tsn_ro"
DBPASS_PROFILE = "Qyppak-jahbut-1tymtu"
DBHOST_PROFILE = "10.60.43.32"
DBPORT_PROFILE = 5432
DBNAME_PROFILE = 'l2m'

DBUSER = "postgres"
DBPASS = "P_Q3=8hWULQnPMx6W5;GX.c;"
DBHOST = "10.50.44.116"
DBPORT = 30568
DBNAME = 'l2m'

if __name__ == "__main__":
    spark = SparkSession.builder.appName('L2M clan info').getOrCreate()
    
    # 3. Chạy ETL cho ngày chưa có
    clan_info_query = f"""(
            with data as (SELECT distinct actor_str3 as user_id, 
                actor_id, 
                actor_name, 
                actor_world, 
                actor_level, 
                first_value(actor_str2) over (partition by actor_str3, actor_id order by logtime desc) as clan_name, 
                first_value(actor_guild) over (partition by actor_str3, actor_id order by logtime desc) as clan_id,
                first_value(ds) over (partition by actor_str3, actor_id order by logtime desc) as last_log_record
                FROM iceberg.l2m.etl_increase_money where entity_id = '900000001' and DATE(logtime + INTERVAL '7' hour) >= now() - interval '8' day
                and actor_guild != '0'),
                rn as (select *, row_number() OVER(partition by user_id order by actor_level desc) as rn from data)
                select * from rn where rn = 1
            ) AS tmp"""

    clan_info = (
            spark.read
                .format("jdbc")
                .option("url", "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE")
                .option("dbtable", clan_info_query)
                .option("user", "sonph4")
                .option("password", "nSaXM0IE3XF8;xo-Bnah")
                .option("driver", "io.trino.jdbc.TrinoDriver")
                .load()
        )

    clan_info.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}") \
            .option("dbtable", "public.clan_info") \
            .option("user", DBUSER) \
            .option("password", DBPASS) \
            .option("driver", "org.postgresql.Driver") \
            .save()