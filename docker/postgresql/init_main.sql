/*OLTP for Clothes E-Shopping*/

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    passwd TEXT NOT NULL,
    date_of_birth DATE,
    sex CHAR(1) CHECK (sex IN ('M', 'F')),
    phone TEXT CHECK (phone ~ '^\+?[0-9]{10,15}$'),
    full_name TEXT CHECK (full_name ~ '^[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+$'),
    registered_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE addresses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    country TEXT CHECK (country ~ '^[A-Z][a-z]+([ -][A-Z][a-z]+)*$'),
    city TEXT CHECK (city ~ '^[A-Z][a-z]+( [A-Z][a-z]+)*$'),
    street TEXT CHECK (street ~ '^[A-Z][a-z]+( [A-Z][a-z]+)*$'),
    house TEXT CHECK (house ~ '^[0-9]+[A-Z]?$'),
    apartment TEXT CHECK (apartment ~ '^[0-9]+[A-Z]?$'),
    zip_code TEXT CHECK (zip_code ~ '^[0-9]{5,10}$'),

    CONSTRAINT fk_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE categories (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT UNIQUE NOT NULL CHECK (name ~ '^[A-Z][a-z]+( [A-Z][a-z]+)*$')
);

-- Товары
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL CHECK (name ~ '^[A-Z][a-z]+( [A-Z][a-z]+)*$') UNIQUE,
    description TEXT,
    category_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,

    CONSTRAINT fk_category
        FOREIGN KEY (category_id) REFERENCES categories(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE INDEX idx_products_name ON products(name);

CREATE TABLE product_variants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    size VARCHAR(10) NOT NULL, -- Varchar because we know which sizes could be
    color VARCHAR(50) NOT NULL, -- Varchar (same as size)
    stock_quantity INTEGER DEFAULT 0,

    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES products(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    CONSTRAINT uniq_product_variant
        UNIQUE (product_id, size, color)
);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    address_id UUID,
    status VARCHAR(50) CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_address
        FOREIGN KEY (address_id) REFERENCES addresses(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    product_variant_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),

    CONSTRAINT fk_order
        FOREIGN KEY (order_id) REFERENCES orders(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_product_variant
        FOREIGN KEY (product_variant_id) REFERENCES product_variants(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

CREATE TABLE payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID UNIQUE,
    payment_method VARCHAR(50) CHECK (payment_method IN ('card', 'paypal', 'sbp')) NOT NULL,
    paid_at TIMESTAMP,
    overall NUMERIC(10, 2) NOT NULL,
    status VARCHAR(50) CHECK (status IN ('unpaid', 'paid', 'failed', 'refunded')) DEFAULT 'unpaid' ,

    CONSTRAINT fk_order
        FOREIGN KEY (order_id) REFERENCES orders(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

GRANT USAGE, CREATE ON SCHEMA public TO airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow;