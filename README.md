# UKTHV — Olist E-Commerce Analytics

**Capstone 2 | Data Visualization & Analytics**
**Sector:** Retail / E-Commerce
**Dataset:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)

---

## Business Problem

> **"How can Olist reduce customer churn and improve seller performance to grow revenue across its e-commerce marketplace?"**

Olist is Brazil's largest department store marketplace. This project analyses transactional data from 2016–2018 to uncover patterns in customer retention, seller quality, delivery performance, and payment behaviour — delivering actionable recommendations for business growth.

---

## Team

| Name | GitHub |
|---|---|
| Sai Thrishul | [@crazylogic03](https://github.com/crazylogic03) |
| Burra Karthikeya | [@Karthikeya1500](https://github.com/Karthikeya1500) |
| Hariksh Suryawanshi | [@Hariksh](https://github.com/Hariksh) |
| Uday Kumar Choudhary | [@Uday-Choudhary](https://github.com/Uday-Choudhary) |
| Vamsi | [@vamsi2246](https://github.com/vamsi2246) |

---

## Dataset

| Property | Detail |
|---|---|
| **Source** | [Kaggle — Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| **Total Files** | 9 raw CSV files |
| **Total Rows** | ~100,000+ orders |
| **Columns** | 40+ analytical columns across all tables |
| **Date Range** | September 2016 – October 2018 |
| **Format** | Row-level transactional records |

---

## Repository Structure

```
UKTHV/
├── README.md
├── data/
│   ├── raw/                          # Original, unedited source datasets
│   └── processed/                    # Cleaned & analysis-ready outputs
├── notebooks/
│   ├── 01_overview.ipynb             # Dataset profiling & quality assessment
│   ├── 02_cleaning.ipynb             # ETL pipeline & data standardisation
│   ├── 03_eda.ipynb                  # Exploratory data analysis
│   ├── 04_statistical_analysis.ipynb # Correlation, regression, hypothesis tests
│   └── 05_final_load_prep.ipynb      # KPI computation & Tableau export
├── scripts/
│   └── etl_pipeline.py               # Standalone ETL pipeline script
├── tableau/
│   ├── screenshots/                  # Dashboard screenshots
│   └── dashboard_links.md            # Tableau Public URL
└── docs/
    └── data_dictionary.md            # Column-level data dictionary
```

---

## How to Run

```bash
# 1. Clone the repository
git clone https://github.com/crazylogic03/UKTHV.git
cd UKTHV

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
.venv\Scripts\activate      # Windows

# 3. Install dependencies
pip install pandas numpy matplotlib seaborn scipy jupyter

# 4. Launch Jupyter
jupyter notebook

# 5. Run notebooks in order: 01 → 02 → 03 → 04 → 05
```

---

## Key KPIs Analysed

- **Customer Churn Rate** — % of customers who never reordered
- **Average Order Value (AOV)** — Mean revenue per order
- **On-Time Delivery Rate** — % orders delivered before estimated date
- **Average Review Score** — Customer satisfaction metric
- **Seller Performance Score** — Composite metric per seller

---

## Tableau Dashboard

See [`tableau/dashboard_links.md`](tableau/dashboard_links.md) for the live interactive dashboard.

---

## Findings Summary

- ~97% of customers are one-time buyers — a critical churn problem
- Late deliveries strongly correlate with low review scores (r = -0.32)
- Credit card is the dominant payment method (~74% of orders)
- Seller response time is a strong predictor of customer satisfaction
- North-East Brazil has the highest delivery delay rates

---

## Academic Integrity

All analysis, code, and recommendations are the original work of the UKTHV team.
No pre-cleaned datasets, copied dashboards, or reused templates were used.
