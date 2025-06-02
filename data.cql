-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS ecommerce 
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS ecommerce.sales (
    product_name TEXT,
    sale_timestamp TIMESTAMP,
    quantity INT,
    price DOUBLE,
    total_value DOUBLE,
    PRIMARY KEY (product_name, sale_timestamp)
);


-- Insert some data
// INSERT INTO ecommerce.sales (product_name, total_sales) 
// VALUES ('TV', 85.88);
