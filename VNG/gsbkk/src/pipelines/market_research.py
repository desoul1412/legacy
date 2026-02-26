"""
Market Research Pipeline - API Extraction Only
Sensor Tower API → HDFS (Raw JSON)

Each API endpoint can be executed independently to prevent DAG failures.
For ETL processing (raw → etl → std → cons), use layout files in /layouts/sensortower/
"""

import json
import logging
from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession

from src.apis.sensortower_api import SensorTowerAPI
from src.core.writers import HadoopWriter

logger = logging.getLogger(__name__)


class MarketResearchPipeline:
    """
    Market research API extraction pipeline using Sensor Tower

    Supported extraction steps:
    - extract_top_games: Fetch top games by country
    - extract_new_games: Fetch newly released games
    - extract_metadata: Fetch app metadata for discovered games
    - extract_performance: Fetch monthly performance metrics

    ETL Processing Steps (use layouts instead):
    - raw → etl: Use layouts/sensortower/etl/{table}.json
    - etl → std: Use layouts/sensortower/std/{table}.json
    - std → cons: Use layouts/sensortower/cons/market_insights.json

    Where {table} is one of: top_games, new_games, metadata, performance
    """

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize pipeline

        Args:
            spark: SparkSession
            config: Pipeline configuration with 'token' and optional 'hadoop_base_path'
        """
        self.spark = spark
        self.config = config
        self.api_client = SensorTowerAPI()
        self.hadoop_writer = HadoopWriter(
            config.get('hadoop_base_path',
                       'hdfs://c0s/user/gsbkk-workspace-yc9t6')
        )

    def run(self, date: str, step: str):
        """
        Run API extraction step

        Args:
            date: Date in YYYY-MM format
            step: Which step to run - 'extract_top_games', 'extract_new_games',
                  'extract_metadata', 'extract_performance'
            countries: List of country codes (default from config)
        """
        # if countries is None:
        #     countries = self.config.get('countries', ['VN', 'TH', 'US'])
        countries = ['VN']

        logger.info(
            f"Starting Market Research extraction for {date}, step={step}")

        # Route to appropriate extraction step
        if step == 'extract_top_games':
            self.extract_top_games(date)
        elif step == 'extract_new_games':
            self.extract_new_games(date)
        elif step == 'extract_metadata':
            self.extract_metadata(date)
        elif step == 'extract_performance':
            self.extract_performance(date)
        else:
            raise ValueError(
                f"Unknown step: {step}. "
                f"Valid steps: extract_top_games, extract_new_games, "
                f"extract_metadata, extract_performance"
            )

        logger.info(f"Completed extraction step '{step}' for {date}")

    def extract_top_games(self, date: str):
        """
        Step 1a: Extract top games from Sensor Tower API

        Fetches top 1000 games for each country from both iOS and Android,
        then writes combined data to separate paths per country.
        Uses fixed countries: TH, VN, PH, US

        Args:
            date: Date in YYYY-MM format
        """
        logger.info(f"Extracting top games for {date}")

        # Fixed countries list
        # fixed_countries = ['TH', 'VN', 'PH', 'US']
        fixed_countries = ['VN']  # one country for testing
        successful_countries = []

        for country in fixed_countries:
            logger.info(f"Fetching top 1000 games for {country}")

            try:
                # Fetch iOS top games
                logger.info(f"Fetching iOS top games for {country}")
                ios_top_games = self.api_client.get_top_apps(
                    start_date=date + '-01',
                    end_date=self._get_month_end_date(date),
                    region=country,
                    os='ios'
                )

                # Fetch Android top games
                logger.info(f"Fetching Android top games for {country}")
                android_top_games = self.api_client.get_top_apps(
                    start_date=date + '-01',
                    end_date=self._get_month_end_date(date),
                    region=country,
                    os='android'
                )

                # Combine iOS and Android data into one flat list
                all_top_games = []
                if ios_top_games:
                    all_top_games.extend(ios_top_games)
                if android_top_games:
                    all_top_games.extend(android_top_games)

                if all_top_games:
                    # Convert each record to JSON line (one {} per line)
                    json_lines = [json.dumps(record)
                                  for record in all_top_games]
                    rdd = self.spark.sparkContext.parallelize(json_lines)
                    df_top = self.spark.read.json(rdd)

                    # Write to HDFS (separate path for each country)
                    path = f"sensortower/raw/top_games/{country}/{date}"
                    self.hadoop_writer.write(
                        df_top, path, mode='overwrite', format='json')
                    logger.info(
                        f"Saved {len(all_top_games)} top games (iOS + Android) for {country} to {path}")
                    successful_countries.append(country)
                else:
                    logger.warning(f"No top games data returned for {country}")

            except Exception as e:
                logger.error(
                    f"Failed to fetch top games for {country}: {e}", exc_info=True)
                # Continue with other countries instead of failing entire task
                continue

        if successful_countries:
            logger.info(
                f"Successfully saved top games data for {len(successful_countries)} countries: {', '.join(successful_countries)}")
        else:
            logger.error("No top games data collected from any country")
            raise ValueError(
                "Failed to fetch top games data for all countries")

    def extract_new_games(self, date: str):
        """
        Step 1b: Extract new games and their performance from Sensor Tower API

        Flow:
        1. Fetch new game IDs from iOS and Android
        2. For each new game, fetch performance data for the launch month
        3. Write performance data to HDFS (not the IDs)

        Args:
            date: Date in YYYY-MM format
        """
        logger.info(f"Extracting new games and their performance for {date}")

        # Target countries (currently 'VN' for testing)
        target_countries = ['VN']

        try:
            # Fetch iOS new games (with pagination)
            logger.info(f"Fetching iOS new games")
            ios_result = self.api_client.get_new_apps(
                start_date=date + '-01',
                os='ios'
            )

            # Fetch Android new games (with pagination)
            logger.info(f"Fetching Android new games")
            android_result = self.api_client.get_new_apps(
                start_date=date + '-01',
                os='android'
            )

            # Combine iOS and Android IDs
            combined_ids = []
            if ios_result and 'ids' in ios_result:
                combined_ids.extend(ios_result['ids'])
            if android_result and 'ids' in android_result:
                combined_ids.extend(android_result['ids'])

            if not combined_ids:
                logger.warning(f"No new games data returned")
                return

            logger.info(
                f"Found {len(combined_ids)} new games ({len(ios_result.get('ids', []))} iOS + {len(android_result.get('ids', []))} Android)")

            # For new games, fetch performance from start of month to end of month
            start_date = date + '-01'
            end_date = self._get_month_end_date(date)

            logger.info(
                f"Fetching performance data for {len(combined_ids)} new games from {start_date} to {end_date}")

            # Separate iOS (numeric) and Android (non-numeric) app_ids
            ios_app_ids = [
                app_id for app_id in combined_ids if str(app_id).isdigit()]
            android_app_ids = [
                app_id for app_id in combined_ids if not str(app_id).isdigit()]

            logger.info(
                f"Split into {len(ios_app_ids)} iOS apps and {len(android_app_ids)} Android apps")

            performance_results = []

            # Fetch iOS performance data
            if ios_app_ids:
                logger.info(
                    f"Fetching performance for {len(ios_app_ids)} iOS apps")
                ios_results = self.api_client.get_performance(
                    app_ids=ios_app_ids,
                    os='ios',
                    start_date=start_date,
                    end_date=end_date
                )
                if ios_results:
                    performance_results.extend(ios_results)
                    logger.info(
                        f"Fetched {len(ios_results)} iOS performance records")

            # Fetch Android performance data
            if android_app_ids:
                logger.info(
                    f"Fetching performance for {len(android_app_ids)} Android apps")
                android_results = self.api_client.get_performance(
                    app_ids=android_app_ids,
                    os='android',
                    start_date=start_date,
                    end_date=end_date
                )
                if android_results:
                    performance_results.extend(android_results)
                    logger.info(
                        f"Fetched {len(android_results)} Android performance records")

            if performance_results:
                # Convert each record to JSON line (one {} per line)
                json_lines = [json.dumps(record)
                              for record in performance_results]
                rdd = self.spark.sparkContext.parallelize(json_lines)
                df_perf = self.spark.read.json(rdd)
                # df_perf = self.spark.createDataFrame(performance_results)
                path = f"sensortower/raw/new_games/{date}"
                self.hadoop_writer.write(
                    df_perf, path, mode='overwrite', format='json')
                logger.info(
                    f"Saved {len(performance_results)} performance records for new games to {path}")
            else:
                logger.warning("No performance data returned for new games")

        except Exception as e:
            logger.error(
                f"Failed to fetch new games performance: {e}", exc_info=True)

    def extract_metadata(self, date: str):
        """
        Step 2: Extract app metadata for all discovered games (top games + new games)
        that don't already exist in the dim_app table.

        IMPORTANT: This step should run AFTER top_games/new_games are processed to CONS layer.

        Flow:
        1. Reads app_ids with country and OS from CONS layer (clean, flattened data)
        2. Filters out app IDs that already exist in PostgreSQL dim_app table
        3. Fetches metadata from TWO endpoints in PARALLEL:
           - /{os}/apps: App details (100 apps per call, by country and os)
           - /app_tag/tags_for_apps: Game tags (100 apps per call)
        4. Saves both responses to separate JSON files in HDFS RAW layer
        5. Then process metadata through ETL → STD layers using layout files

        Dependencies:
        - Requires: sensortower/cons/market_insights/{date} (from top_games + new_games)
        - Outputs: sensortower/raw/metadata_apps/{date}, sensortower/raw/metadata_tags/{date}

        Optimization: Minimizes API requests by skipping games we already have metadata for.

        Args:
            date: Date in YYYY-MM format
            countries: List of country codes
        """
        logger.info(
            f"Extracting metadata for all games (top + new) discovered in {date}")

# Read from PostgreSQL mart (consolidated market insights)
        # This is much simpler than parsing nested JSON from RAW layer
        # List of dicts: {'app_id': str, 'country': str, 'os': str}
        all_apps_info = []

        try:
            # Read from PostgreSQL mart table
            db_config = self.config.get('postgres_db', {})
            # schema = db_config.get('schema', 'public')
            # mart_table = db_config.get('mart_table', 'mart_market_insights')

            logger.info(
                f"Reading app info from PostgreSQL table: sensortower.public.market_insights")

            # Query to get distinct app_id, country, os combinations for the specified date
            # Using LEFT JOIN with NULL check - much faster than NOT IN
            # Make sure both tables have indexes on (app_id, country) for best performance

            query = f"""
                SELECT DISTINCT
                    app_id,
                    country
                FROM test.dim_app_variants
            """

            dim_app_variants = (
                self.spark.read.format("jdbc")
                .option(
                    "url",
                    "jdbc:postgresql://10.50.44.116:30568/sensortower",
                )
                .option("query", query)
                .option("user", 'postgres')
                .option("password", 'P_Q3=8hWULQnPMx6W5;GX.c;')
                .option("driver", "org.postgresql.Driver")
                .load()
            )
            path = f"sensortower/cons/market_insights"
            market_insights_df = self.spark.read.format('parquet').load(
                f"{self.hadoop_writer.base_path}/{path}"
            )

            result = market_insights_df.join(
                dim_app_variants,
                on=["app_id", "country"],
                how="left_anti"
            ).select("app_id", "country").distinct()

            # Convert query results to list of dicts
            # Call .collect() to get rows from DataFrame
            for row in result.collect():
                app_id = str(row.app_id)
                country = row.country
                # Infer OS from app_id (numeric = iOS, non-numeric = Android)
                os_value = 'ios' if str(row.app_id).isdigit() else 'android'

                all_apps_info.append({
                    'app_id': app_id,
                    'country': country,
                    'os': os_value
                })

            logger.info(
                f"Collected {len(all_apps_info)} app records from PostgreSQL mart")

        except Exception as e:
            logger.error(
                f"Failed to read from PostgreSQL mart: {e}", exc_info=True)
            logger.error(
                "IMPORTANT: Make sure the mart table exists and contains data for the specified date!")
            raise

        if not all_apps_info:
            logger.warning("No app info collected")
            return

        # Deduplicate by (app_id, country, os)
        unique_apps = {}
        for app in all_apps_info:
            key = (app['app_id'], app['country'], app['os'])
            unique_apps[key] = app
        all_apps_info = list(unique_apps.values())

        logger.info(
            f"Collected {len(all_apps_info)} unique app records (app_id + country + os combinations)")

        # All apps in all_apps_info are NEW apps that need metadata
        apps_to_fetch = all_apps_info
        app_ids_to_fetch = list(set(app['app_id'] for app in apps_to_fetch))

        logger.info(
            f"Fetching metadata for {len(app_ids_to_fetch)} unique apps ({len(apps_to_fetch)} app-country-os combinations)")
        logger.info(
            f"Total API calls needed: {len(apps_to_fetch)} app records across countries/OS")

        # === Fetch from BOTH endpoints in PARALLEL using threads ===
        from collections import defaultdict
        from concurrent.futures import ThreadPoolExecutor, as_completed

        all_apps_metadata = []
        all_game_tags = []

        def fetch_apps_metadata():
            """Thread function to fetch app details from /{os}/apps endpoint"""
            apps_metadata_list = []
            try:
                # Group by (country, os) and batch by 100 app_ids
                apps_by_country_os = defaultdict(list)
                for app in apps_to_fetch:
                    key = (app['country'], app['os'])
                    apps_by_country_os[key].append(app['app_id'])

                for (country, os), app_ids_list in apps_by_country_os.items():
                    logger.info(
                        f"[Thread 1] Fetching app details for {len(app_ids_list)} apps in {country}/{os}")

                    # Batch into groups of 100
                    for i in range(0, len(app_ids_list), 100):
                        batch = app_ids_list[i:i+100]
                        try:
                            response = self.api_client.get_apps_metadata(
                                app_ids=batch,
                                country=country,
                                os=os
                            )
                            if response:
                                apps_metadata_list.extend(response)
                                logger.info(
                                    f"  [Thread 1] Fetched {len(response)} apps from {country}/{os} (batch {i//100 + 1})")
                        except Exception as e:
                            logger.error(
                                f"  [Thread 1] Failed to fetch app details batch for {country}/{os}: {e}")
                            continue

                logger.info(
                    f"[Thread 1] Completed: fetched {len(apps_metadata_list)} total app details")
                return apps_metadata_list

            except Exception as e:
                logger.error(
                    f"[Thread 1] Failed to fetch app details metadata: {e}", exc_info=True)
                return []

        def fetch_game_tags():
            """Thread function to fetch game tags from /app_tag/tags_for_apps endpoint"""
            game_tags_list = []
            try:
                # Group app_ids by OS first (iOS vs Android)
                # iOS = numeric app_ids, Android = non-numeric app_ids
                ios_app_ids = [
                    app_id for app_id in app_ids_to_fetch if str(app_id).isdigit()]
                android_app_ids = [
                    app_id for app_id in app_ids_to_fetch if not str(app_id).isdigit()]

                logger.info(
                    f"[Thread 2] Fetching game tags for {len(app_ids_to_fetch)} apps ({len(ios_app_ids)} iOS, {len(android_app_ids)} Android)")

                # Batch iOS app_ids into groups of 100
                if ios_app_ids:
                    logger.info(
                        f"[Thread 2] Processing {len(ios_app_ids)} iOS apps")
                    for i in range(0, len(ios_app_ids), 100):
                        batch = ios_app_ids[i:i+100]
                        try:
                            response = self.api_client.get_game_tags(
                                app_ids=batch)
                            if response:
                                game_tags_list.extend(response)
                                logger.info(
                                    f"  [Thread 2] Fetched game tags for {len(response)} iOS apps (batch {i//100 + 1})")
                        except Exception as e:
                            logger.error(
                                f"  [Thread 2] Failed to fetch game tags batch for iOS: {e}")
                            continue

                # Batch Android app_ids into groups of 100
                if android_app_ids:
                    logger.info(
                        f"[Thread 2] Processing {len(android_app_ids)} Android apps")
                    for i in range(0, len(android_app_ids), 100):
                        batch = android_app_ids[i:i+100]
                        try:
                            response = self.api_client.get_game_tags(
                                app_ids=batch)
                            if response:
                                game_tags_list.extend(response)
                                logger.info(
                                    f"  [Thread 2] Fetched game tags for {len(response)} Android apps (batch {i//100 + 1})")
                        except Exception as e:
                            logger.error(
                                f"  [Thread 2] Failed to fetch game tags batch for Android: {e}")
                            continue

                logger.info(
                    f"[Thread 2] Completed: fetched {len(game_tags_list)} total game tags")
                return game_tags_list

            except Exception as e:
                logger.error(
                    f"[Thread 2] Failed to fetch game tags: {e}", exc_info=True)
                return []

        # Execute both functions in parallel
        logger.info("Starting parallel fetch from both endpoints...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit both tasks
            future_apps = executor.submit(fetch_apps_metadata)
            future_tags = executor.submit(fetch_game_tags)

            # Wait for both to complete
            all_apps_metadata = future_apps.result()
            all_game_tags = future_tags.result()

        logger.info(
            f"Parallel fetch completed. Apps: {len(all_apps_metadata)}, Tags: {len(all_game_tags)}")

        # === Save apps metadata to HDFS ===
        if all_apps_metadata:
            try:
                # Convert each app record to JSON line (one record per line, not nested)
                # Response structure from example1.json: {"apps": [app1, app2, ...]}
                # We extract the apps array and save each app as a separate line
                json_lines = [json.dumps(app) for app in all_apps_metadata]
                rdd = self.spark.sparkContext.parallelize(json_lines)
                df_apps = self.spark.read.json(rdd)

                df_apps_metadata = df_apps.select(
                    "unified_app_id",
                    "app_id",
                    "canonical_country",
                    "country_release_date",
                    "description",
                    "icon_url",
                    "name",
                    "os",
                    "publisher_name",
                    "publisher_country"
                )

                path = f"sensortower/raw/metadata/{date}-01"
                self.hadoop_writer.write(
                    df_apps_metadata, path, mode='overwrite', format='json')
                logger.info(
                    f"Saved app details for {len(all_apps_metadata)} apps to {path}")
            except Exception as e:
                logger.error(
                    f"Failed to save app details to HDFS: {e}", exc_info=True)
        else:
            logger.warning("No app details metadata returned from API")

        # === Save game tags to HDFS ===
        if all_game_tags:
            try:
                # Convert each tag record to JSON line (one record per line, not nested)
                # Response structure from example.json: {"data": [tag1, tag2, ...]}
                # We extract the data array and save each tag record as a separate line
                json_lines = [json.dumps(tag) for tag in all_game_tags]
                rdd = self.spark.sparkContext.parallelize(json_lines)
                df_tags_raw = self.spark.read.json(rdd)

                # Explode tags array: 1 row with N tags -> N rows with 1 tag each
                # Then flatten the tag struct to get name, value, exact_value columns
                from pyspark.sql.functions import col, explode
                df_tags_exploded = df_tags_raw.select(
                    "app_id",
                    explode("tags").alias("tag_struct")
                )

                df_tags = df_tags_exploded.select(
                    "app_id",
                    col("tag_struct.name").alias("tag_name"),
                    col("tag_struct.value").alias("tag_value"),
                    col("tag_struct.exact_value").alias("tag_exact_value")
                )

                path = f"sensortower/raw/game_tags/{date}-01"
                self.hadoop_writer.write(
                    df_tags, path, mode='overwrite', format='json')
                logger.info(
                    f"Saved game tags for {len(all_game_tags)} apps to {path}")
            except Exception as e:
                logger.error(
                    f"Failed to save game tags to HDFS: {e}", exc_info=True)
        else:
            logger.warning("No game tags returned from API")

    def extract_performance(self, date: str):
        """
        Step 1d: Extract performance metrics (90-day incremental collection)

        Tracks performance per (app_id, country) combination. For each app in each country,
        checks if it has collected 90 days of data yet. If not, fetches the remaining days.

        This works for:
        - New games that just launched (get launching month performance)
        - Any game (including new and top games) that hasn't reached 90 days yet (incremental fetch)
        - Different countries may have different amounts of data for the same app

        Args:
            date: Date in YYYY-MM format
        """
        logger.info(f"Extracting performance metrics for {date}")

        # Collect all (app_id, country) combinations from both top_games and new_games
        all_app_country_pairs = set()  # Set of (app_id, country) tuples

        # Read top games
        try:
            path = f"sensortower/std/top_games"
            df = self.spark.read.format('parquet').load(
                f"{self.hadoop_writer.base_path}/{path}")
            # Get distinct (app_id, country) pairs
            pairs = [(row.app_id, row.country) for row in df.select(
                'app_id', 'country').distinct().collect()]
            all_app_country_pairs.update(pairs)
        except Exception as e:
            logger.warning(f"Could not read top_games: {e}")

        # Read new games
        try:
            path = f"sensortower/std/new_games"
            df = self.spark.read.format('parquet').load(
                f"{self.hadoop_writer.base_path}/{path}")
            # Get distinct (app_id, country) pairs
            pairs = [(row.app_id, row.country) for row in df.select(
                'app_id', 'country').distinct().collect()]
            all_app_country_pairs.update(pairs)
        except Exception as e:
            logger.warning(f"Could not read new_games: {e}")

        if not all_app_country_pairs:
            logger.warning(
                "No app-country pairs found to fetch performance for")
            return

        logger.info(
            f"Found {len(all_app_country_pairs)} total app-country pairs")

        # Filter to only games released on or after 2025-01-01 (reduce load)
        try:
            logger.info(
                "Filtering to only games with release_date >= 2025-01-01...")

            # Query to get all (app_id, country) pairs with release_date >= 2025-01-01
            query = """
                SELECT DISTINCT app_id, country
                FROM test.dim_app_variants
                WHERE release_date >= '2025-01-01'
            """

            recent_apps_df = (
                self.spark.read.format("jdbc")
                .option(
                    "url",
                    "jdbc:postgresql://10.50.44.116:30568/sensortower",
                )
                .option("query", query)
                .option("user", 'postgres')
                .option("password", 'P_Q3=8hWULQnPMx6W5;GX.c;')
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            # Convert to set of tuples for fast lookup
            recent_apps_set = {(row.app_id, row.country)
                               for row in recent_apps_df.collect()}

            # Filter all_app_country_pairs to only include recent games
            all_app_country_pairs = all_app_country_pairs.intersection(
                recent_apps_set)

            logger.info(
                f"After filtering: {len(all_app_country_pairs)} app-country pairs with release_date >= 2025-01-01")

            if not all_app_country_pairs:
                logger.info(
                    "No recent games (>= 2025-01-01) found to fetch performance for")
                return

        except Exception as e:
            logger.error(
                f"Failed to filter by release date: {e}", exc_info=True)
            logger.warning("Proceeding with unfiltered list")

        logger.info(
            f"Checking performance status for {len(all_app_country_pairs)} app-country pairs")

        # For each (app_id, country), determine if it needs more performance data (< 90 days)
        # Use multi-threading to check status in parallel (can handle thousands of apps)
        # (app_id, country) -> {'launch_date', 'last_perf_date', 'days_collected'}
        games_needing_performance = {}

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def check_app_status(app_id, country):
            """Thread function to check single app status"""
            try:
                app_info = self._get_app_performance_status(app_id, country)

                if not app_info:
                    logger.warning(
                        f"Could not get performance status for app {app_id} in {country}, skipping")
                    return None

                days_collected = app_info['days_collected']

                # Check if game needs more data (< 90 days)
                if days_collected < 90:
                    logger.debug(
                        f"App {app_id} in {country}: {days_collected}/90 days collected, needs {90 - days_collected} more days")
                    return ((app_id, country), app_info)
                else:
                    logger.debug(
                        f"App {app_id} in {country}: Already has {days_collected} days (>=90), skipping")
                    return None

            except Exception as e:
                logger.warning(
                    f"Error checking performance status for app {app_id} in {country}: {e}")
                return None

        # Use ThreadPoolExecutor to check multiple apps in parallel
        logger.info("Checking app status in parallel using multi-threading...")
        with ThreadPoolExecutor(max_workers=6) as executor:
            # Submit all tasks
            futures = {executor.submit(check_app_status, app_id, country): (app_id, country)
                       for app_id, country in all_app_country_pairs}

            # Collect results as they complete
            completed_count = 0
            for future in as_completed(futures):
                result = future.result()
                if result:
                    key, info = result
                    games_needing_performance[key] = info

                completed_count += 1
                if completed_count % 100 == 0:
                    logger.info(
                        f"Status check progress: {completed_count}/{len(all_app_country_pairs)}")

        logger.info(
            f"Completed status check for {len(all_app_country_pairs)} app-country pairs")

        if not games_needing_performance:
            logger.info(
                "All app-country pairs already have 90 days of performance data. No fetching needed.")
            return

        logger.info(
            f"Fetching performance for {len(games_needing_performance)} app-country pairs that need more data")

        # Calculate personalized date ranges for each app and group by (country, os, start_date, end_date)
        from collections import defaultdict
        from datetime import datetime, timedelta

        # Group structure: (country, os, start_date, end_date) -> [app_id1, app_id2, ...]
        apps_by_date_range = defaultdict(list)

        for (app_id, country), info in games_needing_performance.items():
            launch_date = datetime.strptime(
                info['launch_date'][:10], '%Y-%m-%d')
            last_perf_date = datetime.strptime(
                info['last_perf_date'][:10], '%Y-%m-%d')
            days_collected = info['days_collected']

            # Calculate how many more days we need (up to 90 total)
            days_needed = 90 - days_collected

            # Calculate start date (day after last_perf_date)
            if days_collected == 0:
                # First fetch: start from launch date
                fetch_start_date = launch_date
            else:
                # Incremental fetch: start from day after last performance date
                fetch_start_date = last_perf_date + timedelta(days=1)

            # Calculate end date (fetch up to days_needed, but not beyond today)
            today = datetime.now()
            fetch_end_date = min(
                fetch_start_date + timedelta(days=days_needed - 1),
                today
            )

            # Convert to strings
            start_date_str = fetch_start_date.strftime('%Y-%m-%d')
            end_date_str = fetch_end_date.strftime('%Y-%m-%d')

            # Determine OS from app_id
            os_value = 'ios' if str(app_id).isdigit() else 'android'

            # Group by (country, os, start_date, end_date)
            key = (country, os_value, start_date_str, end_date_str)
            apps_by_date_range[key].append(app_id)

        logger.info(
            f"Grouped apps into {len(apps_by_date_range)} unique date range batches")

        # Fetch performance data for each date range batch in parallel
        try:
            all_performance_results = []

            def fetch_batch_performance(country, os, start_date, end_date, app_ids):
                """Thread function to fetch performance for one batch (same country, os, date range)"""
                try:
                    logger.info(
                        f"[{country}/{os}] Fetching {len(app_ids)} apps from {start_date} to {end_date}")

                    results = self.api_client.get_performance(
                        app_ids=app_ids,
                        os=os,
                        start_date=start_date,
                        end_date=end_date,
                        date_granularity='daily',
                        country=country
                    )

                    if results:
                        logger.info(
                            f"[{country}/{os}] Fetched {len(results)} records for {len(app_ids)} apps ({start_date} to {end_date})")
                        return results
                    else:
                        logger.warning(
                            f"[{country}/{os}] No data returned for batch")
                        return []

                except Exception as e:
                    logger.error(
                        f"[{country}/{os}] Failed to fetch batch ({start_date} to {end_date}): {e}",
                        exc_info=True)
                    return []

            logger.info(
                f"Fetching performance data for {len(apps_by_date_range)} batches in parallel...")

            # Use ThreadPoolExecutor to fetch multiple batches in parallel
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {
                    executor.submit(fetch_batch_performance, country, os, start_date, end_date, app_ids):
                    (country, os, start_date, end_date)
                    for (country, os, start_date, end_date), app_ids in apps_by_date_range.items()
                }

                for future in as_completed(futures):
                    batch_key = futures[future]
                    try:
                        batch_results = future.result()
                        all_performance_results.extend(batch_results)
                        logger.info(
                            f"Batch completed: {len(batch_results)} records for {batch_key}")
                    except Exception as e:
                        logger.error(
                            f"Batch failed {batch_key}: {e}", exc_info=True)

            if all_performance_results:
                # Convert each record to JSON line (one {} per line)
                json_lines = [json.dumps(record)
                              for record in all_performance_results]
                rdd = self.spark.sparkContext.parallelize(json_lines)
                df_perf = self.spark.read.json(rdd)

                # Add month partition column from date field
                from pyspark.sql.functions import col, date_format
                df_perf_with_month = df_perf.withColumn(
                    "month",
                    date_format(col("d"), "yyyy-MM-01")
                )

                # Write partitioned by month
                path = f"sensortower/raw/daily_performance"
                self.hadoop_writer.write(
                    df_perf_with_month, path, mode='append', format='json', partition_by=['month'])
                logger.info(
                    f"Saved {len(all_performance_results)} performance records partitioned by month")
            else:
                logger.warning("No performance data returned from API")

        except Exception as e:
            logger.error(
                f"Failed to fetch performance metrics: {e}", exc_info=True)

    # ============ HELPER METHODS ============

    def _get_app_performance_status(self, app_id: str, country: str) -> Dict:
        """
        Get an app's performance collection status for a specific country
        (launch date, last perf date, days collected)

        Args:
            app_id: App ID to check
            country: Country code to check (e.g., 'VN', 'US')

        Returns:
            Dict with keys: launch_date, last_perf_date, days_collected
            None if app not found in metadata table for this country
        """
        try:
            # Step 1: Query PostgreSQL for release_date (launch date)
            query = f"""
                SELECT release_date
                FROM test.dim_app_variants
                WHERE app_id = '{app_id}' AND country = '{country}'
            """

            dim_result_df = (
                self.spark.read.format("jdbc")
                .option(
                    "url",
                    "jdbc:postgresql://10.50.44.116:30568/sensortower",
                )
                .option("query", query)
                .option("user", 'postgres')
                .option("password", 'P_Q3=8hWULQnPMx6W5;GX.c;')
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            dim_results = dim_result_df.collect()
            if not dim_results or len(dim_results) == 0:
                logger.warning(
                    f"App {app_id} in {country} not found in metadata table")
                return None

            launch_date = dim_results[0]['release_date']

            # Step 2: Read daily_performance from Hadoop for this app_id and country
            # Handle case where the table doesn't exist yet (first run)
            last_perf_date = None
            days_collected = 0

            try:
                path = f"sensortower/std/daily_performance/*/*"
                daily_performance_df = self.spark.read.format('parquet').load(path)

                # Filter for specific app_id and country, get max date
                from pyspark.sql.functions import max as spark_max
                perf_filtered = daily_performance_df.filter(
                    (daily_performance_df.app_id == app_id) &
                    (daily_performance_df.country == country)
                )

                # Get max date
                max_date_row = perf_filtered.agg(
                    spark_max("date").alias("max_date")).collect()

                if max_date_row and max_date_row[0]['max_date'] is not None:
                    last_perf_date = max_date_row[0]['max_date']
                    # Calculate days collected
                    days_collected = self._calculate_days_between(
                        str(launch_date), str(last_perf_date))
                else:
                    # No performance data for this specific app yet, needs all 90 days
                    last_perf_date = launch_date
                    days_collected = 0

            except Exception as e:
                logger.debug(
                    f"Daily performance table not found or error reading: {e}")
                # Table doesn't exist yet (first run) - all apps need their first 90 days
                last_perf_date = launch_date
                days_collected = 0

            return {
                'launch_date': str(launch_date),
                'last_perf_date': str(last_perf_date),
                'days_collected': days_collected
            }

        except Exception as e:
            logger.warning(
                f"Failed to get performance status for app {app_id} in {country}: {e}")
            return None

    def _calculate_days_between(self, start_date: str, end_date: str) -> int:
        """
        Calculate number of days between two dates

        Args:
            start_date: Date in YYYY-MM-DD format
            end_date: Date in YYYY-MM-DD format

        Returns:
            Number of days between (inclusive)
        """
        try:
            from datetime import datetime
            start = datetime.strptime(start_date[:10], '%Y-%m-%d')
            end = datetime.strptime(end_date[:10], '%Y-%m-%d')
            return (end - start).days + 1
        except Exception as e:
            logger.warning(
                f"Failed to calculate days between {start_date} and {end_date}: {e}")
            return 0

    def _get_month_end_date(self, date: str) -> str:
        """
        Get the last day of a month

        Args:
            date: Date in YYYY-MM format

        Returns:
            Date in YYYY-MM-DD format (last day of month)
        """
        try:
            from datetime import datetime
            year, month = int(date[:4]), int(date[5:7])

            # Last day of month: move to next month, subtract 1 day
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)

            from datetime import timedelta
            last_day = next_month - timedelta(days=1)

            return last_day.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Failed to get month end date for {date}: {e}")
            # Fallback to -28
            return date + '-28'
