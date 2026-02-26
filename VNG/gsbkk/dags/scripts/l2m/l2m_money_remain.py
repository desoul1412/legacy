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
    spark = SparkSession.builder.appName('L2M money remain').getOrCreate()
    
    # 3. Chạy ETL cho ngày chưa có
    money_remain_query = f"""(
            WITH spend AS (
    SELECT 
        DATE(logtime + INTERVAL '7' hour) AS report_date, logtime,
        actor_str3,
        new_num1
    FROM iceberg.l2m.etl_decrease_money
    WHERE actor_world LIKE '140%'
    and new_num1 != 0
      AND CAST(DATE(logtime + INTERVAL '7' hour) AS VARCHAR) = raw_date
      and DATE(logtime + INTERVAL '7' hour) >= now() - interval '14' day
    ),
    gain as (
        SELECT 
            DATE(logtime + INTERVAL '7' hour) AS report_date, logtime,
            actor_str3,
            new_num1
        FROM iceberg.l2m.etl_increase_money
        WHERE actor_world LIKE '140%'
        and new_num1 != 0
          AND CAST(DATE(logtime + INTERVAL '7' hour) AS VARCHAR) = raw_date
          and DATE(logtime + INTERVAL '7' hour) >= now() - interval '14' day),
    data as (select * from spend union all select * from gain)
    SELECT distinct
        actor_str3 AS user_id,
        LAST_VALUE(new_num1) OVER (
            PARTITION BY actor_str3 
            ORDER BY logtime
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS remain,
        LAST_VALUE(report_date) OVER (
            PARTITION BY actor_str3 
            ORDER BY logtime
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_recard_date
    FROM data
    ORDER BY 2 desc,1) AS tmp"""

    remain = (
            spark.read
                .format("jdbc")
                .option("url", "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE")
                .option("dbtable", money_remain_query)
                .option("user", "sonph4")
                .option("password", "nSaXM0IE3XF8;xo-Bnah")
                .option("driver", "io.trino.jdbc.TrinoDriver")
                .load()
        )

    remain.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}") \
            .option("dbtable", "public.remain") \
            .option("user", DBUSER) \
            .option("password", DBPASS) \
            .option("driver", "org.postgresql.Driver") \
            .save()