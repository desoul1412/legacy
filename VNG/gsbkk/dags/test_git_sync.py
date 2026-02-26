"""
Test DAG to verify Airflow is synced with latest git commit
Shows current git commit hash and contents of recently modified files
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'baolsn2',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'test_git_sync',
    default_args=default_args,
    description='Test if Airflow has synced latest git commits',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'git', 'debug']
)

# Task 1: Show current git commit
show_git_commit = BashOperator(
    task_id='show_git_commit',
    bash_command='''
    echo "=== GIT SYNC CHECK ==="
    echo "Current directory: $(pwd)"
    cd /opt/airflow/dags/repo
    echo "Git commit hash: $(git rev-parse HEAD)"
    echo "Git commit message: $(git log -1 --oneline)"
    echo "Git branch: $(git branch --show-current)"
    echo ""
    echo "Expected commit: a431eaf (Fix remaining INT to DATE conversions in WHERE clauses)"
    echo ""
    echo "Recent commits:"
    git log --oneline -5
    ''',
    dag=dag
)

# Task 2: Check if cumulative_revenue template exists and show first 10 lines
check_cumrev_template = BashOperator(
    task_id='check_cumulative_revenue_template',
    bash_command='''
    echo "=== CUMULATIVE REVENUE TEMPLATE CHECK ==="
    TEMPLATE_PATH="/opt/airflow/dags/repo/templates/re-standardization/l2m/cumulative_revenue.sql.j2"
    if [ -f "$TEMPLATE_PATH" ]; then
        echo "✓ Template exists at: $TEMPLATE_PATH"
        echo ""
        echo "First 15 lines:"
        head -n 15 "$TEMPLATE_PATH"
    else
        echo "✗ Template NOT FOUND at: $TEMPLATE_PATH"
        echo "This means git sync failed or templates not deployed"
    fi
    ''',
    dag=dag
)

# Task 3: Check cumulative_revenue layout file
check_cumrev_layout = BashOperator(
    task_id='check_cumulative_revenue_layout',
    bash_command='''
    echo "=== CUMULATIVE REVENUE LAYOUT CHECK ==="
    LAYOUT_PATH="/opt/airflow/dags/repo/layouts/re-standardization/l2m/cons/l2m_cumulative_revenue.json"
    if [ -f "$LAYOUT_PATH" ]; then
        echo "✓ Layout exists at: $LAYOUT_PATH"
        echo ""
        echo "Content:"
        cat "$LAYOUT_PATH"
        echo ""
        echo "Checking for sqlTemplate field:"
        if grep -q "sqlTemplate" "$LAYOUT_PATH"; then
            echo "✓ CORRECT: Layout uses sqlTemplate (new approach)"
            grep "sqlTemplate" "$LAYOUT_PATH"
        else
            echo "✗ WRONG: Layout missing sqlTemplate (old code still running!)"
        fi
    else
        echo "✗ Layout NOT FOUND at: $LAYOUT_PATH"
    fi
    ''',
    dag=dag
)

# Task 4: Check all template files exist
check_all_templates = BashOperator(
    task_id='check_all_template_files',
    bash_command='''
    echo "=== ALL L2M CONS TEMPLATES CHECK ==="
    cd /opt/airflow/dags/repo/templates/re-standardization/l2m
    echo "Templates in directory:"
    ls -lh *.sql.j2 2>/dev/null || echo "No .sql.j2 files found!"
    echo ""
    echo "Expected templates:"
    for template in active.sql.j2 recharge_cons.sql.j2 cumulative_revenue.sql.j2 server_performance.sql.j2 farmers_performance.sql.j2; do
        if [ -f "$template" ]; then
            echo "✓ $template exists"
        else
            echo "✗ $template MISSING"
        fi
    done
    ''',
    dag=dag
)

# Task 5: Check Python etl_engine.py has latest code
check_etl_engine = BashOperator(
    task_id='check_etl_engine_code',
    bash_command='''
    echo "=== ETL ENGINE CHECK ==="
    ENGINE_PATH="/opt/airflow/dags/repo/src/etl_engine.py"
    if [ -f "$ENGINE_PATH" ]; then
        echo "✓ etl_engine.py exists"
        echo ""
        echo "Checking for table existence check (added in recent commits):"
        if grep -q "DatabaseMetaData.getTables" "$ENGINE_PATH"; then
            echo "✓ CORRECT: Contains DatabaseMetaData.getTables (new code)"
        else
            echo "✗ OLD CODE: Missing DatabaseMetaData.getTables"
        fi
        echo ""
        echo "Line count: $(wc -l < $ENGINE_PATH)"
    else
        echo "✗ etl_engine.py NOT FOUND"
    fi
    ''',
    dag=dag
)

# Task 6: Check file timestamps
check_file_timestamps = BashOperator(
    task_id='check_file_timestamps',
    bash_command='''
    echo "=== FILE MODIFICATION TIMESTAMPS ==="
    cd /opt/airflow/dags/repo
    echo "Last modified times for key files:"
    ls -lh --time-style=full-iso layouts/re-standardization/l2m/cons/l2m_cumulative_revenue.json 2>/dev/null || echo "Layout not found"
    ls -lh --time-style=full-iso templates/re-standardization/l2m/cumulative_revenue.sql.j2 2>/dev/null || echo "Template not found"
    ls -lh --time-style=full-iso src/etl_engine.py 2>/dev/null || echo "etl_engine.py not found"
    ''',
    dag=dag
)

# Set task dependencies
show_git_commit >> [check_cumrev_template, check_cumrev_layout, check_all_templates, check_etl_engine, check_file_timestamps]
