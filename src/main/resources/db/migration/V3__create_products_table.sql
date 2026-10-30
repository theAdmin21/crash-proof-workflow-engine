CREATE TABLE IF NOT EXISTS products (
    sku VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO products (sku, name, price, stock_quantity) VALUES
('SKU-MACBOOK', 'Apple MacBook Pro M3', 150000.00, 10),
('SKU-IPHONE', 'iPhone 16 Pro', 120000.00, 15),
('SKU-SONY', 'Sony WH-1000XM5 Headphones', 25000.00, 30)
ON CONFLICT (sku) DO NOTHING;
