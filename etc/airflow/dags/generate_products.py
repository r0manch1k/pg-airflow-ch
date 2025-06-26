import logging
import random
from datetime import datetime, timedelta

import faker_commerce
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from faker import Faker

logger = logging.getLogger(__name__)


@dag(
    dag_id="generate_products",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Generate products with variants and categories",
    schedule=None,
    start_date=datetime(2025, 6, 24),
    catchup=False,
    tags=["generate"],
)
def GenerateProducts():
    # TODO: Consider using pandas df.to_sql(...) for inserts

    @task
    def generate_categories():
        categories = [
            "Clothing",
            "Shoes",
            "Accessories",
            "Outerwear",
            "Sportswear",
            "Underwear",
            "Swimwear",
            "Formalwear",
            "Casualwear",
            "Workwear",
        ]

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for name in categories:
            cur.execute(
                """
                INSERT INTO categories (name) VALUES (%s)
                ON CONFLICT (name) DO UPDATE
                SET name = EXCLUDED.name;
                """,
                (name,),
            )
            logger.info(f"Inserted or updated category: {name}")

        conn.commit()

    @task
    def generate_products():
        fake = Faker()
        fake.add_provider(faker_commerce.Provider)
        Faker.seed(19)
        random.seed(19)

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id FROM categories")
        category_ids_all = [row[0] for row in cur.fetchall()]

        clothes = [
            "Classic Shirt",
            "Denim Jeans",
            "Leather Jacket",
            "Summer Dress",
            "Wool Sweater",
            "Cotton Shirt",
            "Chino Pants",
            "Polo Shirt",
            "Blue Blazer",
            "Soft Cardigan",
            "Grey Hoodie",
            "Sport Tracksuit",
            "Casual Shorts",
            "Pleated Skirt",
            "Rain Coat",
            "Puffer Vest",
            "White Tanktop",
            "Cargo Pants",
            "Warm Turtleneck",
            "Bomber Jacket",
        ]

        n_products = 20
        for _ in range(n_products):
            name = clothes.pop()
            description = fake.text(max_nb_chars=100)
            category_id = random.choice(category_ids_all)
            cur.execute(
                """
                INSERT INTO products (name, description, category_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category_id = EXCLUDED.category_id;
                """,
                (name, description, category_id),
            )
            logger.info(f"Inserted or updated product: {name}")

        conn.commit()

    @task
    def generate_products_variants():
        fake = Faker()
        fake.add_provider(faker_commerce.Provider)
        Faker.seed(19)
        random.seed(19)

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id FROM products")
        product_ids_all = [row[0] for row in cur.fetchall()]

        sizes = ["XS", "S", "M", "L", "XL"]
        colors = ["Red", "Blue", "Green", "Black", "White"]

        for pid in product_ids_all:
            for _ in range(random.randint(2, 4)):
                price = round(random.uniform(10, 1000), 2)
                size = random.choice(sizes)
                color = random.choice(colors)
                stock_quantity = random.randint(0, 100)
                cur.execute(
                    """
                    INSERT INTO product_variants (product_id, price, size, color, stock_quantity)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (product_id, size, color) DO UPDATE
                    SET price = EXCLUDED.price,
                        size = EXCLUDED.size,
                        color = EXCLUDED.color,
                        stock_quantity = EXCLUDED.stock_quantity;
                    """,
                    (pid, price, size, color, stock_quantity),
                )
                logger.info(
                    f"Inserted or updated product variant for product ID {pid}: "
                    f"Size {size}, Color {color}, Price {price}, Stock {stock_quantity}"
                )

        conn.commit()

    generate_categories() >> generate_products() >> generate_products_variants()


dag = GenerateProducts()
