from spark_singleton import SparkSingleton
from warehouse import write_to_db, get_currency_mapping
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSingleton.get_spark("L2M Server Performance")

product_code_group_map = {
    "l2m": {
        "JV1 - Lineage 2 - TH": ["TH"],
        "JV1 - Lineage 2 - PH": ["PH"],
        "JV1 - Lineage 2 - ID": ["ID"],
        "JV1 - Lineage 2 - SG": ["SG"],
        "JV1 - Lineage 2 - MY": ["MY"],
        "JV1 - Lineage 2 - VN": ["VN"]
    }
}

flat_map = {}
for game_id, products in product_code_group_map.items():
    for product_code, countries in products.items():
        for country in countries:
            flat_map[(game_id, country)] = product_code


def lookup_product_code(game_id, country_code):
    return flat_map.get((game_id, country_code), "JV1 - Lineage 2 - OTHER")


product_code_udf = udf(lookup_product_code, StringType())


def get_data():
    user_profile_query = """
        SELECT
            game_id,
            user_id,
            country_code,
            first_login_time,
            last_login_time,
            register_time,
            first_charge_time,
            last_charge_time,
            total_rev
        FROM public.mkt_user_profile
    """
    
    user_active_query = """
        SELECT DISTINCT
            'l2m' as game_id,
            CAST((CAST(logtime AS TIMESTAMP) + interval '7' hour) as date) AS report_date,
            actor_str3 AS user_id,
            actor_world AS server_id
        FROM iceberg.l2m.etl_login
        WHERE actor_world LIKE '%140%'
            and cast((CAST(logtime AS TIMESTAMP) + interval '7' hour) as DATE) = CAST(raw_date as date)
        GROUP BY 1,2,3,4
    """
    
    user_charge_query = """
        WITH raw_transactions AS (
            SELECT
                'l2m' as game_id,
                amount as rev,
                currency_code,
                goods_id AS product_id,
                CAST((CAST(occurred AS TIMESTAMP) + interval '7' hour) as date) AS report_date,
                game_account_id AS user_id,
                game_server_id AS server_id
            from iceberg.l2m.etl_recharge
            where
                name = 'completed'
            and cast((CAST(occurred AS TIMESTAMP) + interval '7' hour) as DATE) = CAST(raw_date as date)
        )
        ,charge AS (
            SELECT
                game_id,
                currency_code,
                report_date,
                user_id,
                server_id,
                sum(rev) AS rev
            FROM raw_transactions
            GROUP BY 1,2,3,4,5
            )
        SELECT *,
            CASE
            WHEN YEAR(report_date) < YEAR(current_date - INTERVAL '1' day)
                THEN SUBSTR(CAST(report_date AS varchar), 1, 7)
            
            WHEN YEAR(report_date) = YEAR(current_date - INTERVAL '1' day)
                AND MONTH(report_date) < MONTH(current_date - INTERVAL '1' day)
                THEN SUBSTR(CAST(report_date AS varchar), 1, 7)
            
            WHEN MONTH(report_date) = 1
                THEN CAST((year(report_date) - 1) AS varchar) || '-12'
            
            ELSE CAST(year(report_date) AS varchar) || '-' ||
                LPAD(CAST(month(report_date) - 1 AS varchar), 2, '0')
            END AS report_month
        FROM charge
    """
    
    user_profile_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://10.60.43.32:5432/l2m",
        )
        .option("query", user_profile_query)
        .option("user", "tsn_ro")
        .option("password", "Qyppak-jahbut-1tymtu")
        .option("driver", 'org.postgresql.Driver')
        .load()
    )
    print('---- DONE GETTING USER PROFILE ----')
    
    active_performance_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE",
        )
        .option("query", user_active_query)
        .option("user", "trangnm10")
        .option("password", "T2F#r{sFA]A8]ltC2Gi]")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )
    print('---- DONE GETTING ACTIVE PERFORMANCE ----')
    
    charge_performance_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE",
        )
        .option("query", user_charge_query)
        .option("user", "trangnm10")
        .option("password", "T2F#r{sFA]A8]ltC2Gi]")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )
    
    currency_mapping = get_currency_mapping()
    active_performance_df = active_performance_df.withColumn('report_date', to_date(col('report_date')))
    charge_performance_df = charge_performance_df.join(currency_mapping, on=['report_month', 'currency_code'], how='left') \
                                                .withColumn('report_date', to_date(col('report_date'))) \
                                                .withColumn('rev_usd', col('rev') * col('exchange_rate_to_usd')) \
                                                .select('game_id', 'report_date', 'user_id', 'server_id', 'rev_usd')
    print('---- DONE GETTING CHARGE PERFORMANCE ----')
    
    return user_profile_df, active_performance_df, charge_performance_df
    
def server_performance(active, charge, user_profile):
    total_rev = col("total_rev") / 25840
    
    l2m_vip_level = (
        when(total_rev >= 20000, "G. >= $20000")
        .when(total_rev >= 4000, "F. $4000 - $20000")
        .when(total_rev >= 2000, "E. $2000 - $4000")
        .when(total_rev >= 400, "D. $400 - $2000")
        .when(total_rev >= 200, "C. $200 - $400")
        .when(total_rev >= 40, "B. $40 - $200")
        .when(total_rev < 40, "A. < $40")
        .otherwise("Free")
    )

    vip_level_expr = (
        when(col("game_id") == "l2m", l2m_vip_level)
    )
    
    # Build the user_profile DataFrame
    user_profile = user_profile.withColumn(
        "nru_date", to_date(col("first_login_time"))
    ).withColumn(
        "last_login_date", to_date(col("last_login_time"))
    ).withColumn(
        "last_charge_date", to_date(col("last_charge_time"))
    ).withColumn(
        "first_charge_date", to_date(col("first_charge_time"))
    ).withColumn(
        "vip_level", vip_level_expr
    ).withColumn(  # for each country
        "product_code", product_code_udf(col("game_id"), col("country_code"))
    ).select(
        "product_code", "country_code", "user_id", "nru_date", "last_login_date",
        "last_charge_date", "first_charge_date", "vip_level"
    )
    
    # --------------------------------------------------------------------------
    # 2. CTE: user_active
    # --------------------------------------------------------------------------
    user_active = active.join(user_profile, "user_id", "left") \
        .withColumn(  # for each country
            "product_code", product_code_udf(col("game_id"), col("country_code"))
        ) \
        .filter(col("report_date") >= "2025-01-01") \
        .withColumnRenamed("report_date", "date") \
        .withColumn("week", date_trunc('week', col("date"))) \
        .withColumn("month", date_trunc('month', col("date"))) \
        .select("product_code", "user_id", "server_id", "date", "nru_date", "last_login_date",
        "last_charge_date", "first_charge_date", "vip_level") \
        .distinct()

    # --------------------------------------------------------------------------
    # 3. CTE: user_charge
    # --------------------------------------------------------------------------
    user_charge = charge.join(user_profile, "user_id", "left") \
        .withColumn(  # for each country
            "product_code", product_code_udf(col("game_id"), col("country_code"))
        ) \
        .filter(col("report_date") >= "2025-01-01") \
        .withColumnRenamed("report_date", "date") \
        .withColumn("week", date_trunc('week', col("date"))) \
        .withColumn("month", date_trunc('month', col("date"))) \
        .select("product_code", "user_id", "server_id", "date", "rev_usd", "nru_date", "last_login_date",
        "last_charge_date", "first_charge_date", "vip_level")
        
    # --------------------------------------------------------------------------
    # 4. CTE: active_users
    # --------------------------------------------------------------------------

    # Define conditional expression for NRU
    nru_expr = when(
        col("nru_date") == col("date"), col("user_id")
    )

    active_users = user_active.groupBy(
            "product_code", "date", "vip_level", "server_id"
        ) \
        .agg(
            countDistinct("user_id").alias("dau"),
            countDistinct(nru_expr).alias("nru")
        )
        
    # --------------------------------------------------------------------------
    # 5. CTE: paying_users
    # --------------------------------------------------------------------------
    # Add helper column for first_charge_month
    joined_charge = user_charge

    # Define conditional expressions for aggregation
    is_npu = col("first_charge_date") == col("date")
    is_nnpu = (col("first_charge_date") == col("date")) & (col("nru_date") == col("date"))

    paying_users = joined_charge.groupBy(
            "product_code", "date", "vip_level", "server_id"
        ) \
        .agg(
            countDistinct("user_id").alias("pu"),
            countDistinct(when(is_npu, col("user_id"))).alias("npu"),
            countDistinct(when(is_nnpu, col("user_id"))).alias("nnpu"),
            sum("rev_usd").alias("rev_usd"),
            sum(when(is_npu, col("rev_usd"))).alias("rev_npu"),
            sum(when(is_nnpu, col("rev_usd"))).alias("rev_nnpu")
        )
        
    # --------------------------------------------------------------------------
    # 6. Final Select
    # --------------------------------------------------------------------------
    join_keys = [
        "product_code", "date", "server_id", "vip_level"
    ]

    server_performance_df = active_users.join(paying_users, join_keys, "left")\
        .select(
            'product_code', 'date', 'server_id', 'vip_level', 'dau', 'nru', 'pu', 'npu', 'nnpu', 'rev_usd', 'rev_npu', 'rev_nnpu'
        )
    
    return server_performance_df

def execute():
    user_profile_df, active_performance_df, charge_performance_df = get_data()
    server_performance_df = server_performance(active_performance_df, charge_performance_df, user_profile_df)
    
    write_to_db(
        df=server_performance_df,
        dbname="l2m",
        dbtable="public.l2m_server_performance",
        mode="overwrite",
    )
    
if __name__ == "__main__":
    execute()