from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_singleton import SparkSingleton
from warehouse import write_to_db, get_currency_mapping
from constants import Credential

spark = SparkSingleton.get_spark("L2M Farmers Performance")

def get_charge_data():
    charge_performance_query = """
        WITH raw_transactions AS (
            SELECT
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
                currency_code,
                report_date,
                user_id,
                server_id,
                sum(rev) AS rev
            FROM raw_transactions
            GROUP BY 1,2,3,4
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
    
    charge_performance_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE",
        )
        .option("query", charge_performance_query)
        .option("user", "trangnm10")
        .option("password", "T2F#r{sFA]A8]ltC2Gi]")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )
    
    currency_mapping = get_currency_mapping()
    charge_performance_df = charge_performance_df.join(currency_mapping, on=['report_month', 'currency_code'], how='left') \
                                                .withColumn('rev_usd', col('rev') * col('exchange_rate_to_usd')) \
                                                .select('report_date', 'user_id', 'server_id', 'rev_usd')
    print('---- DONE GETTING CHARGE PERFORMANCE ----')
    
    return charge_performance_df

def get_active_data():
    active_performance_query = """
        SELECT DISTINCT
            CAST((CAST(logtime AS TIMESTAMP) + interval '7' hour) as date) AS report_date,
            actor_str3 AS user_id,
            actor_world AS server_id
        FROM iceberg.l2m.etl_login
        WHERE actor_world LIKE '%140%'
            and cast((CAST(logtime AS TIMESTAMP) + interval '7' hour) as DATE) = CAST(raw_date as date)
        GROUP BY 1,2,3
    """
    
    active_performance_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE",
        )
        .option("query", active_performance_query)
        .option("user", "trangnm10")
        .option("password", "T2F#r{sFA]A8]ltC2Gi]")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )
    print('---- DONE GETTING ACTIVE PERFORMANCE ----')
    
    return active_performance_df

def get_farmers_snapshot():
    farmers_snapshot_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://{Credential.POSTGRES_TSN['host']}:{Credential.POSTGRES_TSN['port']}/l2m",
        )
        .option("query", "SELECT distinct user_id, user_type FROM l2m_farmers_snapshot")
        .option("user", Credential.POSTGRES_TSN["user"])
        .option("password", Credential.POSTGRES_TSN["pass"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    print('---- DONE GETTING FARMERS SNAPSHOT ----')
    
    user_profile_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://{Credential.POSTGRES_GDS['host']}:{Credential.POSTGRES_GDS['port']}/l2m",
        )
        .option("query", """
            SELECT DISTINCT
                user_id,
                country_code,
                platform,
                first_login_time,
                first_charge_time,
                last_login_time,
                last_charge_time
            FROM mkt_user_profile
            WHERE first_login_time IS NOT NULL
        """)
        .option("user", Credential.POSTGRES_GDS["user"])
        .option("password", Credential.POSTGRES_GDS["pass"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    print('---- DONE GETTING USER PROFILE ----')
    
    return farmers_snapshot_df, user_profile_df


def process(active_df, charge_df, farmers_profile, user_profile):
    charge_joined_df = charge_df.join(user_profile, on='user_id', how='left') \
                        .join(farmers_profile, on='user_id', how='left')
                          
    active_joined_df = active_df.join(user_profile, on='user_id', how='left') \
                        .join(farmers_profile, on='user_id', how='left')
                      
    charge_df = charge_joined_df.withColumn('first_login_date', to_date(col('first_login_time'))) \
                        .withColumn('first_charge_date', to_date(col('first_charge_time'))) \
                        .withColumn('last_login_date', to_date(col('last_login_time'))) \
                        .withColumn('last_charge_date', to_date(col('last_charge_time')))
                        
    active_df = active_joined_df.withColumn('first_login_date', to_date(col('first_login_time'))) \
                        .withColumn('first_charge_date', to_date(col('first_charge_time'))) \
                        .withColumn('last_login_date', to_date(col('last_login_time'))) \
                        .withColumn('last_charge_date', to_date(col('last_charge_time')))

    active_df_with_flags = (
        active_df # all rows are paying users since it's a charge table
        .withColumn("is_nru", when(col("first_login_date") == col("report_date"), 1).otherwise(0))
    )

    # Add flags for PU, NPU, NNPU
    charge_df_with_flags = (
        charge_df
        .withColumn("is_pu", lit(1))  # all rows are paying users since it's a charge table
        .withColumn("is_nru", when(col("first_login_date") == col("report_date"), 1).otherwise(0))
        .withColumn("is_npu", when(col("first_charge_date") == col("report_date"), 1).otherwise(0))
        .withColumn("is_nnpu", when((col("first_charge_date") == col("report_date")) &
                                    (col("first_login_date") == col("report_date")), 1).otherwise(0))
    )

    # Aggregate to get PU, NPU, NNPU, revenues
    agg_charge_df = (
        charge_df_with_flags
        .groupBy("report_date", "user_type", "first_login_date", "server_id", "country_code", "platform")
        .agg(
            countDistinct("user_id").alias("PU"),
            countDistinct(when(col("is_npu") == 1, col("user_id"))).alias("NPU"),
            countDistinct(when(col("is_nnpu") == 1, col("user_id"))).alias("NNPU"),
            sum("rev_usd").alias("rev_usd"),
            sum(when(col("is_nru") == 1, col("rev_usd")).otherwise(0)).alias("rev_nru"),
            sum(when(col("is_npu") == 1, col("rev_usd")).otherwise(0)).alias("rev_npu"),
            sum(when(col("is_nnpu") == 1, col("rev_usd")).otherwise(0)).alias("rev_nnpu")
        )
    )
                    
    agg_active_df = active_df_with_flags.groupBy("report_date", "first_login_date", "user_type", "server_id", "country_code", "platform")\
                        .agg(
                            countDistinct('user_id').alias('DAU'),
                            countDistinct(when(col("is_nru") == 1, col("user_id"))).alias("NRU")
                        )
                        
    performance_df = agg_active_df.join(agg_charge_df, on=["report_date", "user_type", "first_login_date", "server_id", "country_code", "platform"], how='left')
    return performance_df

def execute():
    charge_df = get_charge_data()
    active_df = get_active_data()
    farmers_snapshot_df, user_profile_df = get_farmers_snapshot()
    
    performance_df = process(active_df, charge_df, farmers_snapshot_df, user_profile_df)
    write_to_db(df=performance_df, dbname='l2m', dbtable='public.l2m_daily_performance', mode='overwrite')
    
if __name__ == '__main__':
    execute()
    