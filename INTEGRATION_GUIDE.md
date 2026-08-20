# DataGuard Multi-Source Integration

DataGuard now supports a reusable **Integration Studio** in addition to multi-source ingestion and Validation Studio.

## Demo flow

1. Open **Data Sources** and add `customers.sql`, `orders.sql`, `products.sql` (or CSV/Excel/JSON/API equivalents).
2. Open **Integration Studio**.
3. Create a project such as **Sales Analysis**.
4. Select the three datasets and choose `orders` as the base dataset.
5. Add a left join from `orders.customer_id` to `customers.customer_id`.
6. Add another left join from the current combined result `product_id` to `products.product_id`.
7. DataGuard suggests compatible join keys and can normalize mismatched key datatypes to text for the join.
8. Click **Build Combined Dataset**.
9. DataGuard creates a separate `Sales_Analysis_combined` dataset, automatically runs default validation on the combined data, and shows an integration + quality report.
10. The combined dataset becomes the active dataset and can be explored, fixed in Validation Studio, revalidated, and exported.

## Architecture

```text
CSV / Excel / JSON / Parquet / SQL / REST / SQLite
                         |
                  Ingestion Registry
                         |
                 Integration Studio
                         |
             Schema / Key Harmonization
                         |
                   Join Pipeline
                         |
               Combined Dataset
                         |
              Default Validation
                         |
              Valid / Invalid / Q
                         |
               Validation Studio
                         |
                Fix + Revalidate
                         |
                    Analytics
```

The original uploaded datasets are never overwritten by integration. The combined dataset is registered as a new source so it can be selected independently.
