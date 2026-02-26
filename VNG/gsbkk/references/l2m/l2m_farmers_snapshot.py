from constants import Credential
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_singleton import SparkSingleton
from warehouse import write_to_db

spark = SparkSingleton.get_spark("L2M Farmers Snapshot")


def build_farmers_snapshot():
    print("-- GETTING ALL FARMERS --")
    farmers_query = """
        WITH cond_farmers AS (
            SELECT data_str1, raw_date
            FROM iceberg.l2m.etl_login
            GROUP BY 1,2
            HAVING COUNT(DISTINCT actor_str3) >= 10
        )
        ,flagged_farmers AS (
            SELECT DISTINCT actor_str3
            FROM iceberg.l2m.etl_login
            JOIN cond_farmers USING(raw_date, data_str1)
            UNION ALL
            SELECT DISTINCT actor_str3
            FROM iceberg.l2m.etl_increase_item eii
            GROUP BY 1
            HAVING MAX(actor_level) = 1
        )
        ,farmers AS (
            SELECT DISTINCT
                actor_str3 AS user_id,
                actor_id AS role_id,
                MAX(actor_level) OVER(PARTITION BY actor_id) AS role_level,
                actor_world AS server_id,
                CASE WHEN actor_str3 IN (SELECT DISTINCT game_account_id AS user_id FROM iceberg.l2m.etl_recharge) 
                    THEN 'PU Farmer' ELSE 'Non-PU Farmer' END AS user_type
            FROM iceberg.l2m.etl_login l join flagged_farmers f USING(actor_str3)
            WHERE (data_str1 NOT IN (
                '103.15.62',
                '103.15.63',
                '211.189.163',
                '211.189.167',
                '27.110.34',
                '59.124.94',
                '203.75.255',
                '66.162.136',
                '209.234.187',
                '62.172.43',
                '64.25.32',
                '66.195.99',
                '4.38.69',
                '52.9.237',
                '10.101.7'
                ) OR data_str1 not like '1.53.255.%') 
            AND actor_str3 IN (SELECT actor_str3 FROM flagged_farmers)
            )
            ,normal AS (
                SELECT DISTINCT
                    actor_str3 AS user_id,
                    actor_id AS role_id,
                    MAX(actor_level) OVER(PARTITION BY actor_id) AS role_level,
                    actor_world AS server_id,
                    'Normal' AS user_type
                FROM iceberg.l2m.etl_login
                WHERE actor_str3 NOT IN (SELECT DISTINCT user_id FROM farmers)
            )
            SELECT * FROM farmers
            UNION ALL
            SELECT * FROM normal
    """

    all_farmers_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:trino://gio-gds-trino.vnggames.net:8080/iceberg/default?SSL=true&SSLVerification=NONE",
        )
        .option("query", farmers_query)
        .option("user", "trangnm10")
        .option("password", "T2F#r{sFA]A8]ltC2Gi]")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )

    print("-- DONE GETTING ALL FARMERS --")
    return all_farmers_df


def get_role_profile():
    print("-- GETTING ROLE PROFILE --")
    role_profile_query = """
        SELECT user_id, role_id, server_id, role_name
        FROM public.mkt_role_profile
    """

    role_profile_df = (
        spark.read.format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://{Credential.POSTGRES_GDS['host']}:{Credential.POSTGRES_GDS['port']}/l2m",
        )
        .option("query", role_profile_query)
        .option("user", Credential.POSTGRES_GDS["user"])
        .option("password", Credential.POSTGRES_GDS["pass"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    print("-- DONE GETTING ROLE PROFILE --")

    return role_profile_df


def process():
    # get role name
    all_farmers_df = build_farmers_snapshot()
    role_profile_df = get_role_profile()
    farmers_snapshot_df = (
        all_farmers_df.join(
            role_profile_df, on=["user_id", "role_id", "server_id"], how="left"
        )
        .select(
            "user_id", "role_id", "role_name", "role_level", "server_id", "user_type"
        )
        .withColumn("updated_at", current_timestamp())
    )

    print("-- DONE BUILDING FARMERS SNAPSHOT --")

    write_to_db(
        df=farmers_snapshot_df,
        dbname="l2m",
        dbtable="public.l2m_farmers_snapshot",
        mode="overwrite",
    )


if __name__ == "__main__":
    process()
