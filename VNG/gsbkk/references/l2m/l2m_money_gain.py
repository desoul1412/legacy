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

start_date = datetime.strptime("2025-05-20", "%Y-%m-%d")
end_date   = datetime.today() - timedelta(days=1)

dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date - start_date).days + 1)]

if __name__ == "__main__":
    spark = SparkSession.builder.appName('L2M money gain').getOrCreate()
    jdbc_url = f"jdbc:postgresql://{DBHOST_PROFILE}:{DBPORT_PROFILE}/{DBNAME_PROFILE}"

    user_profile = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "query",
            """
            SELECT 
                user_id,
                CASE 
                    WHEN country_code IN ('TH','VN','ID','PH','SG','MY') 
                        THEN country_code 
                    ELSE 'Other' 
                END AS country_code,
                CASE 
                    WHEN total_rev/26000 >= 20000 THEN 'G. >=20,000$'
                    WHEN total_rev/26000 >= 4000 THEN 'F. 4,000$ - 20,000$'
                    WHEN total_rev/26000 >= 2000 THEN 'E. 2,000$ - 4000$'
                    WHEN total_rev/26000 >= 400 THEN 'D. 400$ - 2000$'
                    WHEN total_rev/26000 >= 200 THEN 'C. 200$ - 400$'
                    WHEN total_rev/26000 >= 40 THEN 'B. 40$ - 200$'
                    WHEN total_rev/26000 BETWEEN 0 AND 40 THEN 'A. <40$'
                    ELSE 'Free'
                END AS vip_level
            FROM public.mkt_user_profile
            """
        )
        .option("user", DBUSER_PROFILE)
        .option("password", DBPASS_PROFILE)
        .load()
    )


    # 1. Lấy ngày đã tồn tại trong Postgres
    existing_dates_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}")
        .option("dbtable", "(SELECT DISTINCT report_date FROM public.money_gain) t")
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
        money_gain_query = f"""(
            SELECT
                ds,
                actor_str3 as user_id,
                actor_world as server,
                log_detail_code as reason_id,
                SUM(use_num2) as use_num2
            FROM iceberg.l2m.etl_increase_money
            WHERE actor_world LIKE '%140%'
            AND ds = DATE'{d}'
            AND entity_id LIKE '9%2'
            GROUP BY 1,2,3,4
        ) AS tmp"""

        df = (
            spark.read
                .format("jdbc")
                .option("url", "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE")
                .option("dbtable", money_gain_query)
                .option("user", "sonph4")
                .option("password", "nSaXM0IE3XF8;xo-Bnah")
                .option("driver", "io.trino.jdbc.TrinoDriver")
                .load()
        )

        daily_gain = (
            df
            .withColumn("report_date", F.col("ds").cast("date"))
            .withColumn("gain_amount", F.coalesce(F.col("use_num2"), F.lit(0)))
            .join(user_profile, how="left", on="user_id")
            .select("report_date","user_id","server","reason_id","gain_amount","country_code","vip_level")
            .groupby("report_date","server","country_code","reason_id","vip_level")
            .agg(F.sum("gain_amount").alias("gain_amount"))
        )

        daily_gain.write.format("jdbc") \
            .mode("append") \
            .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}") \
            .option("dbtable", "public.money_gain") \
            .option("user", DBUSER) \
            .option("password", DBPASS) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        print(f"✅ Done {d}")