# SensorTower API Client - Quick Reference

## Initialization
```python
from src.core.api_client import create_client
import os

client = create_client('sensortower', token=os.getenv('SENSOR_TOWER_API_TOKEN'))
```

## Core Methods (10 Total)

### 1️⃣ Discovery
```python
# Get new app IDs (with pagination)
app_ids = client.get_new_apps(
    os_platform='android',  # or 'ios'
    category='game',        # or '6014' for iOS
    start_date='2024-12-01',
    limit=10000
)
# Returns: List[str] - App IDs
```

### 2️⃣ Metadata (Parallel)
```python
# Fetch metadata + game tags in batches of 100
metadata_df = client.get_metadata_parallel(
    app_ids=['com.game1', 'com.game2'],
    os_platform='android',
    batch_size=100,
    max_workers=3
)
# Returns: DataFrame with 20+ columns (name, publisher, genre, tags, etc.)
```

### 3️⃣ Performance (Parallel)
```python
# Monthly performance for multiple countries
performance_df = client.get_monthly_performance_parallel(
    app_ids=['com.game1'],
    countries=['VN', 'TH', 'ID'],
    start_date='2024-12-01',
    end_date='2024-12-31',
    os_platform='android',
    batch_size=100,
    max_workers=3
)
# Returns: DataFrame with downloads & revenue by country/month
```

### 4️⃣ Top Apps (Single Country)
```python
# Top games by revenue for one country
top_df = client.get_top_apps(
    country='VN',
    start_date='2024-12-01',
    end_date='2024-12-31',
    category='6014',
    measure='revenue',
    limit=1000
)
# Returns: DataFrame with app_id, downloads, revenue, tags
```

### 5️⃣ Top Apps (Multi-Country)
```python
# Fetch multiple countries in parallel
unified_df, all_app_ids = client.fetch_all_countries(
    countries=['VN', 'TH', 'ID', 'PH', 'SG'],
    start_date='2024-12-01',
    end_date='2024-12-31',
    category='6014'
)
# Returns: (DataFrame, List[str]) - Combined data + unique app IDs
```

### 6️⃣ Unified IDs (Auto-detect Platform)
```python
# Get metadata for mixed iOS/Android IDs
metadata_df = client.get_unified_ids(
    app_ids=['123456789', 'com.android.app'],  # Mixed platforms
    batch_size=100,
    max_workers=4
)
# Returns: DataFrame with os='ios'|'android' auto-detected
```

---

## Common Patterns

### Pattern A: New Games Pipeline
```python
# 1. Discover → 2. Metadata → 3. Performance → 4. Merge
app_ids = client.get_new_apps('android', 'game', '2024-12-01')
metadata = client.get_metadata_parallel(app_ids, 'android')
performance = client.get_monthly_performance_parallel(
    app_ids, ['VN'], '2024-12-01', '2024-12-31', 'android'
)
result = metadata.merge(performance, on='app_id')
```

### Pattern B: Top Games Pipeline
```python
# 1. Fetch All Countries → 2. Get Metadata → 3. Merge
top_df, app_ids = client.fetch_all_countries(
    ['VN', 'TH', 'ID'], '2024-12-01', '2024-12-31'
)
metadata = client.get_unified_ids(app_ids)
result = metadata.merge(top_df, on='app_id')
```

---

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `request_delay` | 0.5s | Delay between API requests |
| `batch_delay` | 2.0s | Delay between batch completions |
| `max_retries` | 3 | Max retry attempts |
| `initial_retry_delay` | 1.0s | Starting retry delay |
| `retry_backoff` | 2x | Exponential backoff multiplier |
| `batch_size` | 100 | Apps per batch |
| `max_workers` | 3-4 | Parallel threads |

---

## Platform Detection

**iOS Apps:**
- App ID starts with digit: `123456789`
- Category: `6014`
- Platform: `ios`

**Android Apps:**
- App ID has dots: `com.company.game`
- Category: `game`
- Platform: `android`

**Auto-detected by:** `get_unified_ids()`

---

## Data Columns

### Metadata DataFrame
- `app_id`, `unified_app_id`, `name`, `publisher_name`, `publisher_country`
- `os`, `icon_url`, `url`, `country_release_date`, `rating`, `in_app_purchases`
- `Free`, `Game Genre`, `Game Sub-genre`, `Game Art Style`, `Game Theme`
- `IP: Corporate Parent`, `Revenue First 30 Days (WW)`

### Performance DataFrame (Android)
- `aid` → `app_id`
- `c` → `country_code`
- `d` → `month`
- `u` → `downloads`
- `r` → `revenue` (in cents, divide by 100)

### Performance DataFrame (iOS)
- `aid` → `app_id`
- `cc` → `country_code`
- `d` → `month`
- `iu`, `au` → iPhone/iPad downloads
- `ir`, `ar` → iPhone/iPad revenue (in cents)

### Top Apps DataFrame
- `app_id`, `country`, `month`, `downloads`, `revenue`
- `Free`, `Game Art Style`, `Game Genre`, `Game Sub-genre`, `Game Theme`
- `Revenue First 30 Days (WW)`

---

## Error Handling

**Automatic handling for:**
- ✅ 429 Rate Limiting (exponential backoff)
- ✅ Network timeouts (retry with backoff)
- ✅ Empty responses (return empty DataFrame)
- ✅ Batch failures (continue with other batches)
- ✅ Invalid app IDs (skip and log)

**Logs show:**
- `⚠️` Warnings (retries, rate limits)
- `🔥` Errors (failed batches)
- `✅` Success (completed batches)
- `▶️` Progress (batch start)
- `🟡` Info (no data found)

---

## Performance Tuning

**For High Volume (>10K apps):**
```python
metadata_df = client.get_metadata_parallel(
    app_ids=large_list,
    batch_size=100,    # Keep at 100
    max_workers=3      # Use 3-4 to avoid 429
)
```

**For Many Countries (>10):**
```python
df, ids = client.fetch_all_countries(
    countries=many_countries,  # Fetches in parallel
    ...
)
# Parallelizes by country automatically
```

**For Faster Processing:**
- ✅ Use `max_workers=4` (but watch for 429)
- ✅ Batch multiple operations
- ❌ Don't use `max_workers > 5` (rate limits)

---

## Integration Points

### With DataProcessor
```python
from src.core.data_processor import DataProcessor

# 1. Extract with API client
raw_df = client.get_metadata_parallel(app_ids, 'android')

# 2. Convert to PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(raw_df)

# 3. Transform with DataProcessor
processor = DataProcessor(spark_df, config)
etl_df = processor.process()
```

### With Writers
```python
from src.core.writers import create_writer

# After processing, write to multiple outputs
hadoop_writer = create_writer('hadoop', config)
hadoop_writer.write(processed_df)

postgres_writer = create_writer('postgres', config)
postgres_writer.write(processed_df)
```

---

## Country Codes Reference

**Southeast Asia:**
- `VN` - Vietnam
- `TH` - Thailand
- `ID` - Indonesia
- `PH` - Philippines
- `SG` - Singapore
- `MY` - Malaysia

**East Asia:**
- `JP` - Japan
- `KR` - South Korea
- `CN` - China
- `HK` - Hong Kong
- `TW` - Taiwan
- `MO` - Macau

**Middle East:**
- `SA` - Saudi Arabia
- `AE` - UAE
- `QA` - Qatar
- `OM` - Oman
- `BH` - Bahrain
- `KW` - Kuwait
- `IN` - India

**Other:**
- `US` - United States

---

## Date Formats

**Start/End Dates:** `YYYY-MM-DD`
```python
'2024-12-01'  # December 1, 2024
```

**Month in Response:** `YYYY-MM-DD` or `YYYY-MM`
```python
'2024-12-15'  # Mid-month
'2024-12'     # Whole month
```

---

## Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| 429 errors | Reduce `max_workers` to 2-3 |
| Slow performance | Increase `max_workers` to 4 |
| Empty results | Check date format, category, app_ids |
| Missing columns | Verify API response, check logs |
| Memory issues | Reduce `batch_size` to 50 |

---

## See Also

- **Source Code:** [src/core/api_client.py](../src/core/api_client.py)
- **Main Documentation:** [README.md](../README.md)
- **SensorTower Project:** [layouts/sensortower/README.md](../layouts/sensortower/README.md)
