 CREATE TABLE olist_customers (
     customer_id VARCHAR(50),
     customer_unique_id VARCHAR(50),
     customer_zip_code_prefix INT,
     customer_city VARCHAR(50),
     customer_state CHAR(2)
 );
 CREATE TABLE olist_order_items (
     order_id VARCHAR(50),
     order_item_id INT,
     product_id VARCHAR(50),
     seller_id VARCHAR(50),
     shipping_limit_date TIMESTAMP,
     price DOUBLE PRECISION,
     freight_value DOUBLE PRECISION
 );

 CREATE TABLE olist_order_payments (
     order_id VARCHAR(50),
     payment_sequential INT,
     payment_type VARCHAR(30),
     payment_installments INT,
     payment_value DOUBLE PRECISION
 );
 CREATE TABLE olist_order (
     order_id VARCHAR(50),
     customer_id VARCHAR(50),
     order_status VARCHAR(15),
     order_purchase TIMESTAMP,
     order_approved_at TIMESTAMP,
     order_delivered_carrier_date TIMESTAMP,
     order_delivered_customer_date TIMESTAMP,
     order_estimated_delivery_date TIMESTAMP
 );
 CREATE TABLE olist_products (
     product_id VARCHAR(50),
     product_category_name VARCHAR(50)
 );

 CREATE TABLE olist_sellers (
     seller_id VARCHAR(50),
     seller_zip_code_prefix INT,
     seller_city VARCHAR(50),
     seller_state CHAR(2)
 );

 COPY  olist_sellers
 FROM 's3://olist-dados/olist_sellers_dataset.csv'
 IAM_ROLE 'arn:aws:iam::600627358623:role/service-role/AmazonRedshift-CommandsAccessRole-20250306T114302'
 FORMAT AS CSV
 IGNOREHEADER 1
 DELIMITER ',';

-- Vendas mensais por mes de 2016 a 2018

SELECT
    EXTRACT(YEAR FROM o.order_purchase) AS ano,
    EXTRACT(MONTH FROM o.order_purchase) AS mes,
    ROUND(SUM(oi.price * oi.freight_value)::NUMERIC, 2) AS vendas_mensais
FROM olist_order_items oi
JOIN olist_order o ON oi.order_id = o.order_id
WHERE EXTRACT(YEAR FROM o.order_purchase) BETWEEN 2016 AND 2018
AND o.order_delivered_customer_date IS NOT NULL
GROUP BY ano, mes
ORDER BY ano, mes;

-- Top 10 Categorias de produtos mais vendidas

SELECT 
    p.product_category_name, 
    COUNT(oi.order_id) AS total_vendas
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_vendas DESC
LIMIT 10;

-- Ticket Médio por Categoria de Produto

SELECT 
    p.product_category_name, 
    ROUND(AVG(oi.price), 2) AS ticket_medio
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY ticket_medio DESC;

-- Top 10 Tempo Médio de Entrega por Categoria de Produto

SELECT 
    p.product_category_name, 
    ROUND(AVG(DATEDIFF(day, o.order_purchase, o.order_delivered_customer_date)), 2) AS tempo_medio_entrega_em_dias
FROM olist_order o
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON oi.product_id = p.product_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY p.product_category_name
ORDER BY tempo_medio_entrega_em_dias DESC
LIMIT 10;

-- Distribuição dos Pagamentos por Tipo

SELECT 
    payment_type, 
    COUNT(order_id) AS total_pedidos,
    ROUND(SUM(payment_value), 2) AS total_valor
FROM olist_order_payments
GROUP BY payment_type
ORDER BY total_pedidos DESC;

-- Maiores Vendedores e região (Top 10)

WITH vendas AS (
    SELECT 
        oi.seller_id, 
        os.seller_city, 
        os.seller_state,   Adicionando seller_state
        COUNT(DISTINCT oi.order_id) AS total_vendas,
        ROUND(SUM(oi.price)::NUMERIC, 2) AS total_faturamento
    FROM olist_order_items oi
    JOIN olist_sellers os ON oi.seller_id = os.seller_id  
    GROUP BY oi.seller_id, os.seller_city, os.seller_state
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY total_faturamento DESC) AS vendedor,
    seller_city,  
    seller_state,   Adicionando seller_state no resultado final
    total_vendas,
    total_faturamento
FROM vendas
ORDER BY total_faturamento DESC
LIMIT 10;


 
