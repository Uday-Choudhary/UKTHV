# Data Dictionary — Olist Brazilian E-Commerce Dataset

**Project:** UKTHV Capstone 2 — Retail Analytics
**Dataset Source:** [Kaggle — Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
**Last Updated:** April 2026

---

## Overview

The Olist dataset is a multi-table relational e-commerce dataset covering 100,000+ orders placed on the Olist marketplace between 2016 and 2018. All tables are joined via `order_id` or `customer_id` / `seller_id` keys.

---

## Table 1: `olist_orders_dataset.csv`

**Description:** Core orders table. One row per order. Links to all other tables via `order_id`.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `order_id` | string | Unique identifier for each order | Primary key |
| `customer_id` | string | Unique identifier for the customer who placed the order | Foreign key → customers table |
| `order_status` | categorical | Current status of the order | Values: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved |
| `order_purchase_timestamp` | datetime (string) | Timestamp when the order was placed | Requires parsing to datetime |
| `order_approved_at` | datetime (string) | Timestamp when payment was approved | Has missing values (~160) for canceled orders |
| `order_delivered_carrier_date` | datetime (string) | Date the order was handed to the carrier | Has missing values (~1,783) |
| `order_delivered_customer_date` | datetime (string) | Date the order was delivered to the customer | Has missing values (~2,965) |
| `order_estimated_delivery_date` | datetime (string) | Estimated delivery date shown to customer at purchase | Used for on-time delivery KPI |

**Row Count:** ~99,441
**Missing Values:** Present in delivery date columns (expected for non-delivered orders)

---

## Table 2: `olist_order_items_dataset.csv`

**Description:** Line-item level detail for each order. One row per item in an order.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `order_id` | string | Identifier for the parent order | Foreign key → orders table |
| `order_item_id` | integer | Sequential number identifying each item within an order | Starts at 1 per order |
| `product_id` | string | Identifier for the product | Foreign key → products table |
| `seller_id` | string | Identifier for the seller who fulfilled the item | Foreign key → sellers table |
| `shipping_limit_date` | datetime (string) | Deadline by which the seller must hand the order to the carrier | Requires datetime parsing |
| `price` | float | Item price in BRL | Excludes freight |
| `freight_value` | float | Freight cost for the item in BRL | Wide range — outliers present |

**Row Count:** ~112,650
**Missing Values:** None

---

## Table 3: `olist_order_payments_dataset.csv`

**Description:** Payment details for each order. Multiple rows per order possible (split payments).

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `order_id` | string | Identifier for the parent order | Foreign key → orders table |
| `payment_sequential` | integer | Sequential number for each payment entry on the order | Multiple payments per order allowed |
| `payment_type` | categorical | Payment method used | Values: credit_card, boleto, voucher, debit_card |
| `payment_installments` | integer | Number of installments chosen by the customer | 0 for some vouchers |
| `payment_value` | float | Payment amount in BRL | Some zero-value entries (vouchers) |

**Row Count:** ~103,886
**Missing Values:** None

---

## Table 4: `olist_order_reviews_dataset.csv`

**Description:** Customer reviews submitted after delivery. One review per order (some duplicates).

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `review_id` | string | Unique identifier for each review | |
| `order_id` | string | Identifier for the reviewed order | Foreign key → orders table |
| `review_score` | integer | Star rating given by the customer | Scale: 1–5 |
| `review_comment_title` | string | Short title of the review (optional) | ~58% missing |
| `review_comment_message` | string | Full review text (optional) | ~58% missing |
| `review_creation_date` | datetime (string) | Date the review form was sent to the customer | |
| `review_answer_timestamp` | datetime (string) | Date and time the customer submitted the review | |

**Row Count:** ~99,224
**Missing Values:** High missingness in comment fields (expected — optional)

---

## Table 5: `olist_customers_dataset.csv`

**Description:** Customer profile data. One row per unique customer.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `customer_id` | string | Unique identifier per order (not per person) | Foreign key → orders table |
| `customer_unique_id` | string | True unique customer identifier across multiple orders | Used for churn analysis |
| `customer_zip_code_prefix` | string | First 5 digits of customer zip code | |
| `customer_city` | string | Customer's city | Category inconsistencies present |
| `customer_state` | string | Customer's state (2-letter code) | 27 Brazilian states |

**Row Count:** ~99,441
**Missing Values:** None

---

## Table 6: `olist_products_dataset.csv`

**Description:** Product catalogue data. One row per product.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `product_id` | string | Unique identifier for each product | Primary key |
| `product_category_name` | categorical | Product category name in Portuguese | Has missing values (~610) |
| `product_name_lenght` | integer | Character count of the product name | Has missing values |
| `product_description_lenght` | integer | Character count of the product description | Has missing values |
| `product_photos_qty` | integer | Number of photos listed for the product | Has missing values |
| `product_weight_g` | float | Product weight in grams | |
| `product_length_cm` | float | Product length in cm | |
| `product_height_cm` | float | Product height in cm | |
| `product_width_cm` | float | Product width in cm | |

**Row Count:** ~32,951
**Missing Values:** Present in category name and dimension columns

---

## Table 7: `olist_sellers_dataset.csv`

**Description:** Seller profile data. One row per seller.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `seller_id` | string | Unique identifier for each seller | Primary key |
| `seller_zip_code_prefix` | string | First 5 digits of seller zip code | |
| `seller_city` | string | Seller's city | Category inconsistencies present |
| `seller_state` | string | Seller's state (2-letter code) | |

**Row Count:** ~3,095
**Missing Values:** None

---

## Table 8: `olist_geolocation_dataset.csv`

**Description:** Zip code to latitude/longitude mapping for geographic analysis.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `geolocation_zip_code_prefix` | string | 5-digit zip code prefix | |
| `geolocation_lat` | float | Latitude coordinate | |
| `geolocation_lng` | float | Longitude coordinate | |
| `geolocation_city` | string | City name | |
| `geolocation_state` | string | State (2-letter code) | |

**Row Count:** ~1,000,163
**Missing Values:** None

---

## Table 9: `product_category_name_translation.csv`

**Description:** Portuguese-to-English category name translation table.

| Column | Data Type | Description | Notes |
|---|---|---|---|
| `product_category_name` | string | Category name in Portuguese | Foreign key → products table |
| `product_category_name_english` | string | Category name in English | |

**Row Count:** 71
**Missing Values:** None

---

## Processed Output Files

### `final_dataset.csv`
Master merged dataset combining all 9 tables for analysis. Used in EDA and statistical notebooks.

### `tableau_order_level.csv`
Order-grain dataset with KPIs (AOV, delivery delay, review score, freight %). Designed for Tableau order-level analysis.

### `tableau_customer_level.csv`
Customer-grain dataset with churn flag, total spend, order count, avg review. Designed for Tableau customer segmentation dashboards.

---

## Key Relationships

```
customers ──(customer_id)──> orders ──(order_id)──> order_items ──(product_id)──> products
                                    |                              └──(seller_id)──> sellers
                                    ├──(order_id)──> order_payments
                                    └──(order_id)──> order_reviews
```
