CREATE TABLE warehouse.tnx_customer
(
    transaction_id varchar(128),
    customer_id varchar(128),
    product_id varchar(128),
    quantity int,
    price float,
    timestamp datetime,
    load_date datetime
)
