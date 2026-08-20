# SQL Demo Datasets

These SQL dumps are SQLite-compatible and can be uploaded directly through **Data Sources → Files**.

- `customers.sql` — customer master data with missing email and a negative age.
- `orders.sql` — order data with a missing price, negative quantity, invalid status and duplicate order.
- `products.sql` — product data with a missing price and negative stock.

Uploading one SQL dump registers its table as a DataGuard dataset. If a SQL dump contains multiple tables, DataGuard registers each table separately using `filename_table` as the dataset name.
