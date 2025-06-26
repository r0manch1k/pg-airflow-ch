import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

logger = logging.getLogger(__name__)

with DAG(
    dag_id="migrate_to_clickhouse",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Migrate data from PostgreSQL to ClickHouse",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 6, 24),
    catchup=False,
    tags=["migration"],
) as dag:
    create_users_table = ClickHouseOperator(
        task_id="create_users_table",
        clickhouse_conn_id="main_clickhouse_conn",
        sql=(
            """
                CREATE TABLE IF NOT EXISTS users (
                    id UUID,
                    email String,
                    passwd String,
                    date_of_birth Date,
                    sex String,
                    phone String,
                    full_name String,
                    registered_at DateTime,
                    last_login DateTime,
                    is_active UInt8
                ) ENGINE = MergeTree() ORDER BY id
            """
        ),
    )

    create_addresses_table = ClickHouseOperator(
        task_id="create_addresses_table",
        clickhouse_conn_id="main_clickhouse_conn",
        sql=(
            """
                CREATE TABLE IF NOT EXISTS addresses (
                    id UUID,
                    user_id UUID,
                    country String,
                    city String,
                    street String,
                    house String,
                    apartment String,
                    zip_code String
                ) ENGINE = MergeTree() ORDER BY id
            """
        ),
    )

    create_orders_table = ClickHouseOperator(
        task_id="create_orders_table",
        clickhouse_conn_id="main_clickhouse_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS orders (
                id UUID,
                user_id UUID,
                address_id UUID,
                status String,
                created_at DateTime,
                updated_at DateTime
            ) ENGINE = MergeTree() ORDER BY id
        """,
    )

    create_order_items_table = ClickHouseOperator(
        task_id="create_order_items_table",
        clickhouse_conn_id="main_clickhouse_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS order_items (
                id UUID,
                order_id UUID,
                product_variant_id UUID,
                quantity Int32
            ) ENGINE = MergeTree() ORDER BY id
        """,
    )

    create_payments_table = ClickHouseOperator(
        task_id="create_payments_table",
        clickhouse_conn_id="main_clickhouse_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS payments (
                id UUID,
                order_id UUID,
                payment_method String,
                paid_at DateTime,
                overall Float64,
                status String
            ) ENGINE = MergeTree() ORDER BY id
        """,
    )

    @task
    def migrate_users():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Export
        cur.execute("SELECT * FROM users")
        users = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        # Transform
        df = pd.DataFrame(users, columns=colnames)

        if df.empty:
            logger.info("No users to migrate.")
            return

        df["is_active"] = df["is_active"].astype(int)
        df["date_of_birth"] = pd.to_datetime(
            df["date_of_birth"], errors="coerce"
        ).dt.date
        df["registered_at"] = pd.to_datetime(df["registered_at"], errors="coerce")
        df["last_login"] = pd.to_datetime(df["last_login"], errors="coerce")

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Load
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="main_clickhouse_conn")
        clickhouse_hook.execute("INSERT INTO users VALUES", data)

        logger.info("Users migrated to ClickHouse.")

    @task
    def migrate_addresses():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT * FROM addresses")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.info("No addresses to migrate.")
            return

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="main_clickhouse_conn")
        clickhouse_hook.execute("INSERT INTO addresses VALUES", data)
        logger.info("Addresses migrated to ClickHouse.")

    @task
    def migrate_orders():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT * FROM orders")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.info("No orders to migrate.")
            return

        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="main_clickhouse_conn")
        clickhouse_hook.execute("INSERT INTO orders VALUES", data)
        logger.info("Orders migrated to ClickHouse.")

    @task
    def migrate_order_items():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT * FROM order_items")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.info("No order_items to migrate.")
            return

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="main_clickhouse_conn")
        clickhouse_hook.execute("INSERT INTO order_items VALUES", data)
        logger.info("Order items migrated to ClickHouse.")

    @task
    def migrate_payments():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT * FROM payments")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.info("No payments to migrate.")
            return

        df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")
        df["overall"] = df["overall"].astype(float)

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        clickhouse_hook = ClickHouseHook(clickhouse_conn_id="main_clickhouse_conn")
        clickhouse_hook.execute("INSERT INTO payments VALUES", data)
        logger.info("Payments migrated to ClickHouse.")

    (
        [
            create_users_table,
            create_addresses_table,
            create_orders_table,
            create_order_items_table,
            create_payments_table,
        ]
        >> migrate_users()
        >> migrate_addresses()
        >> migrate_orders()
        >> migrate_order_items()
        >> migrate_payments()
    )
