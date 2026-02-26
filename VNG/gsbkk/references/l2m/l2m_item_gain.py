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

item_ids = [
    "528500006", "528500007", "550200005", "510100011", "518500001",
    "528500003", "528500002", "548500025", "548500024", "548500023",
    "548500022", "548500021", "548500020", "548500019", "548500018",
    "548500010", "548500009", "548500008", "548500007", "548500006",
    "548500005", "548500004", "548500003", "548500002", "548500001",
    "510100032", "511000013", "610340013", "512000007", "510600004",
    "510600003", "510100021", "510100132", "510100014", "510100041",
    "610100001", "930000017", "500200010", "610340011", "610340010",
    "610340009", "510200001", "510200003", "558300004", "558300003",
    "558300002", "558300001", "511100008", "511100004", "511100006",
    "511100002"
]

ids_str = ",".join(f"'{x}'" for x in item_ids)

if __name__ == "__main__":
    spark = SparkSession.builder.appName('L2M Item Gain').getOrCreate()
    jdbc_url = f"jdbc:postgresql://{DBHOST_PROFILE}:{DBPORT_PROFILE}/{DBNAME_PROFILE}"

    user_profile = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", "SELECT user_id, CASE WHEN country_code IN ('TH','VN','ID','PH','SG','MY') then country_code else 'Other' END as country_code FROM public.mkt_user_profile")
                .option("user", DBUSER_PROFILE)
                .option("password", DBPASS_PROFILE)
                .load()
            )

    # 1. Lấy ngày đã tồn tại trong Postgres
    existing_dates_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}")
        .option("dbtable", "(SELECT DISTINCT report_date FROM public.gain) t")
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
        item_gain_query = f"""(
            SELECT
                ds,
                actor_str3 as user_id,
                actor_id as role_id,
                actor_world as server,
                actor_level as role_level,
                entity_id as item_id,
                use_num1
            FROM iceberg.l2m.etl_increase_item
            WHERE actor_world LIKE '%140%'
            AND ds = DATE'{d}'
            AND entity_id IN ({ids_str})
        ) AS tmp"""

        df = (
            spark.read
                .format("jdbc")
                .option("url", "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE")
                .option("dbtable", item_gain_query)
                .option("user", "sonph4")
                .option("password", "nSaXM0IE3XF8;xo-Bnah")
                .option("driver", "io.trino.jdbc.TrinoDriver")
                .load()
        )

        daily_gain = (
            df
            .withColumn("report_date", F.col("ds").cast("date"))
            .withColumn("gain_amount", F.coalesce(F.col("use_num1"), F.lit(0)))
            .join(user_profile, how="left", on="user_id")
            .select("report_date","user_id","server","role_id","role_level","item_id","gain_amount","country_code")
            .groupby("report_date","server","role_level","country_code","item_id")
            .agg(F.sum("gain_amount").alias("gain_amount"))
        )

        daily_gain.write.format("jdbc") \
            .mode("append") \
            .option("url", f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}") \
            .option("dbtable", "public.gain") \
            .option("user", DBUSER) \
            .option("password", DBPASS) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        print(f"✅ Done {d}")