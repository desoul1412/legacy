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

start_date = datetime.strptime("2025-10-01", "%Y-%m-%d")
end_date   = datetime.today() - timedelta(days=1)

dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date - start_date).days + 1)]

if __name__ == "__main__":
    spark = SparkSession.builder.appName('L2M user diamond gain').getOrCreate()

    # 1. Lấy ngày đã tồn tại trong Postgres
    existing_dates_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}")
        .option("dbtable", "(SELECT DISTINCT report_date FROM public.user_diamond_gain) t")
        .option("user", DBUSER)
        .option("password", DBPASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    existing_dates = [row["report_date"].strftime("%Y-%m-%d") for row in existing_dates_df.collect()]

    # 2. Loại bỏ ngày đã có
    pending_dates = [d for d in dates if d not in existing_dates]

    print("✅ Will process:", pending_dates)

    # 3. Chạy ETL cho ngày chưa có
    for d in pending_dates:
        diamond_gain_query = f"""(
            SELECT
                DATE(logtime + INTERVAL '7' HOUR) AS ds,
                actor_str3 as user_id,
                actor_world as server,
                SUM(use_num2) as use_num2
            FROM iceberg.l2m.etl_increase_money
            WHERE actor_world LIKE '%140%'
            AND DATE(logtime + INTERVAL '7' HOUR) = DATE'{d}'
            AND entity_id = '900000002'
            AND CAST(DATE(logtime + INTERVAL '7' HOUR) AS VARCHAR) = raw_date
            GROUP BY 1,2,3
        ) AS tmp"""

        df = (
            spark.read
                .format("jdbc")
                .option("url", "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE")
                .option("dbtable", diamond_gain_query)
                .option("user", "sonph4")
                .option("password", "nSaXM0IE3XF8;xo-Bnah")
                .option("driver", "io.trino.jdbc.TrinoDriver")
                .load()
        )

        user_diamond_gain = (
            df
            .withColumn("report_date", F.col("ds").cast("date"))
            .withColumn("gain_amount", F.coalesce(F.col("use_num2"), F.lit(0)))
            .select("report_date","user_id","server","gain_amount")
            .groupby("user_id","report_date","server")
            .agg(F.sum("gain_amount").alias("gain_amount"))
        )

        user_diamond_gain.write.format("jdbc") \
            .mode("append") \
            .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}") \
            .option("dbtable", "public.user_diamond_gain") \
            .option("user", DBUSER) \
            .option("password", DBPASS) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        print(f"✅ Done {d}")