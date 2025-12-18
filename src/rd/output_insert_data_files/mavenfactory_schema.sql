-- ================================
-- SQLite-compatible CREATE TABLES
-- ================================

-- Table: website_sessions
CREATE TABLE website_sessions (
    website_session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    is_repeat_session INTEGER NOT NULL,
    utm_source TEXT,
    utm_campaign TEXT,
    utm_content TEXT,
    device_type TEXT,
    http_referer TEXT
);
CREATE INDEX idx_website_sessions_user_id ON website_sessions(user_id);

-- Table: website_pageviews
CREATE TABLE website_pageviews (
    website_pageview_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    website_session_id INTEGER NOT NULL,
    pageview_url TEXT NOT NULL
);
CREATE INDEX idx_website_pageviews_session_id ON website_pageviews(website_session_id);

-- Table: products
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    product_name TEXT NOT NULL
);

-- Table: orders
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    website_session_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    primary_product_id INTEGER NOT NULL,
    items_purchased INTEGER NOT NULL,
    price_usd REAL NOT NULL,
    cogs_usd REAL NOT NULL
);
CREATE INDEX idx_orders_website_session_id ON orders(website_session_id);

-- Table: order_items
CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    is_primary_item INTEGER NOT NULL,
    price_usd REAL NOT NULL,
    cogs_usd REAL NOT NULL
);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

-- Table: order_item_refunds
CREATE TABLE order_item_refunds (
    order_item_refund_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    refund_amount_usd REAL NOT NULL
);
CREATE INDEX idx_refunds_order_id ON order_item_refunds(order_id);
CREATE INDEX idx_refunds_order_item_id ON order_item_refunds(order_item_id);
