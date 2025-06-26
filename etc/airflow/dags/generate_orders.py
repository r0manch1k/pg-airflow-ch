import hashlib
import logging
import random
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from faker import Faker

logger = logging.getLogger(__name__)


@dag(
    dag_id="generate_orders",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="Generate users with orders",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 6, 24),
    catchup=False,
    tags=["generate"],
)
def GenerateUserOrders():
    @task
    def generate_user_with_orders():
        fake = Faker()
        Faker.seed(random.randint(1, 10000))
        random.seed(random.randint(1, 10000))

        postgres_hook = PostgresHook(postgres_conn_id="main_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        user_id = fake.uuid4()
        email = fake.unique.email()
        passwd = hashlib.sha256(fake.password(length=12).encode("utf-8")).hexdigest()
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=70)
        sex = random.choice(["M", "F"])
        phone = "+7" + "".join([str(random.randint(0, 9)) for _ in range(10)])
        full_name = f"{fake.last_name()} {fake.first_name()} {fake.first_name()}"
        is_active = True

        cur.execute(
            """
            INSERT INTO users (id, email, passwd, date_of_birth, sex, phone, full_name, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                str(user_id),
                email,
                passwd,
                date_of_birth,
                sex,
                phone,
                full_name,
                is_active,
            ),
        )

        logger.info(f"Inserted user: {full_name} (ID: {user_id})")

        address_id = fake.uuid4()
        country = fake.country()
        city = fake.city()
        street = fake.street_name()
        house = str(random.randint(1, 200))
        apartment = str(random.randint(1, 100))
        zip_code = "".join([str(random.randint(0, 9)) for _ in range(6)])

        cur.execute(
            """
            INSERT INTO addresses (id, user_id, country, city, street, house, apartment, zip_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                str(address_id),
                str(user_id),
                country,
                city,
                street,
                house,
                apartment,
                zip_code,
            ),
        )

        logger.info(
            f"Inserted address for user ID {user_id}: {country}, {city}, {street} {house}, Apt {apartment}, Zip {zip_code}"
        )

        cur.execute("SELECT id FROM product_variants")
        variant_ids = [row[0] for row in cur.fetchall()]
        if not variant_ids:
            raise Exception("Table product_variants is empty!")

        n_orders = random.randint(1, 3)
        for _ in range(n_orders):
            order_id = fake.uuid4()
            status = random.choice(
                ["pending", "paid", "shipped", "delivered", "cancelled"]
            )
            cur.execute(
                """
                INSERT INTO orders (id, user_id, address_id, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (str(order_id), str(user_id), str(address_id), status),
            )

            n_items = random.randint(1, 3)
            used_variants = random.sample(variant_ids, n_items)
            for variant_id in used_variants:
                order_item_id = fake.uuid4()
                quantity = random.randint(1, 5)
                cur.execute(
                    """
                    INSERT INTO order_items (id, order_id, product_variant_id, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (str(order_item_id), str(order_id), str(variant_id), quantity),
                )
                logger.info(
                    f"Inserted order item: Order ID {order_id}, Variant ID {variant_id}, Quantity {quantity}"
                )

            payment_id = fake.uuid4()
            payment_method = random.choice(["card", "paypal", "sbp"])
            paid_at = fake.date_time_this_year()
            overall = round(random.uniform(100, 10000), 2)
            payment_status = random.choice(["unpaid", "paid", "failed", "refunded"])

            cur.execute(
                """
                INSERT INTO payments (id, order_id, payment_method, paid_at, overall, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (
                    str(payment_id),
                    str(order_id),
                    payment_method,
                    paid_at,
                    overall,
                    payment_status,
                ),
            )

            logger.info(
                f"Inserted payment: Order ID {order_id}, Payment ID {payment_id}, Method {payment_method}, "
                f"Paid at {paid_at}, Overall {overall}, Status {payment_status}"
            )

        conn.commit()
        cur.close()
        conn.close()

    generate_user_with_orders()


dag = GenerateUserOrders()
