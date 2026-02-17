# Weekly Amazon Summary Automation

## Project Overview

This project automates the creation of Mockins’ **Weekly Amazon Summary** report.

The goal is to fully replace the current manual Excel-based workflow (including ConnectBooks and intermediate calculation tabs) with a **reproducible, code-driven data pipeline** that:

* Pulls data directly from Amazon and Google Sheets
* Computes all metrics in Python (pandas)
* Outputs a **single, clean Excel file** with one tab: `Weekly Summary`
* Eliminates hidden/helper columns, Excel formulas, and manual steps

This project is intentionally designed as **Phase 1** of a larger data platform that will eventually power dashboards and company-wide analytics.

---

## Output Specification

### Output File

* **Format:** Excel (`.xlsx`)
* **Tabs:** 1
* **Tab Name:** `Weekly Summary`
* **Rows:**

  * Row 1: Human-readable section headers (optional, cosmetic)
  * Row 2: Column headers
  * Row 3+: One row per SKU

### Calculation Rules

* All calculations are performed in **Python (pandas)**
* No Excel formulas
* No hidden or helper columns
* Excel is used strictly as a presentation/output format

---

## Data Grain & Keys

* **Grain:** 1 row = 1 SKU
* **Primary Key:** `SKU`
* **ASIN:** Joined via reference mapping (not a primary key)

---

## Column-by-Column Specification

### A–G: Stock & Shipping Metrics (Computed)

#### A — Current Stock / Weeks

* **Definition:** Weeks of inventory coverage
* **Formula:**

  ```
  (Inventory Available + FC Transfer + FC Processing + Inbound) / 6
  ```
* **Source:** Amazon – Restock Inventory Report

#### B — Amount per Week

* **Definition:** Weekly sales velocity
* **Source:** Computed from Amazon sales windows (Q–AK)

#### C — Times

* **Definition:** Projected units needed for a 6-week horizon
* **Formula:**

  ```
  Amount per Week * 6
  ```

#### D — Pick

* **Definition:** Manual override in legacy workflow
* **Status:** Not automated yet (reserved for future phase)

#### E — Need to Ship

* **Definition:** Units required to meet projected demand
* **Formula:**

  ```
  Times – (Inventory Available + FC Transfer + FC Processing + Inbound)
  ```

#### F — Qty per Carton

* **Definition:** Units per master carton
* **Source:** Google Sheets – *Weights & Dims*

#### G — # of Boxes

* **Definition:** Cartons required to ship
* **Formula:**

  ```
  Need to Ship / Qty per Carton
  ```

---

### H–K: Identity & Pricing

#### H — ASIN

* **Source:** Google Sheets – *Mockins Gross & Net* → `AMZ US`
* **Mapping:** SKU → ASIN

#### I — Mini SKU

* **Definition:** Legacy identifier retained for visibility
* **Source:** Google Sheets – *Mockins Gross & Net* → `AMZ US`

#### J — SKU

* **Definition:** Primary product identifier
* **Source:** Derived from Amazon reports and reference sheets

#### K — Price

* **Source:** Google Sheets – *Mockins Gross & Net* → `AMZ US`

---

### L–P: Amazon FBA Inventory

All inventory values are sourced from **Amazon Restock Inventory Report**.

#### L — Inventory Available

* Sellable FBA units

#### M — FC Transfer

* Units moving between fulfillment centers

#### N — FC Processing

* Units being received or processed by Amazon

#### O — Inbound (Pipeline)

* Units inbound to Amazon FBA
* **Computed two ways for validation:**

  * `Inbound`
  * `Working + Shipped + Receiving`
* Discrepancies are logged

---

### Q–AK: Sales Windows (Units Ordered)

All sales data uses **Units Ordered** from:

* **Amazon – Detail Page Sales & Traffic Report** (via SP-API)
* Replaces ConnectBooks entirely

#### Time Window Rules

* **1 Day = Yesterday (calendar day)**
* All windows are requested explicitly (not inferred)

| Column | Window     |
| ------ | ---------- |
| Q      | 1 Day      |
| X      | 7 Days     |
| Z      | 8–14 Days  |
| AB     | 15–21 Days |
| AD     | 22–28 Days |
| AG     | 29–56 Days |
| AI     | 57–84 Days |

#### Averages

* **AJ — 4 Week Avg** = avg(7d, 8–14, 15–21, 22–28)
* **AK — 3 Month Avg** = avg(1–28, 29–56, 57–84)

---

## Explicit Exclusions

The following are intentionally **not included**:

* Legacy Excel staging tabs
* Hidden helper columns
* Excel formulas
* ConnectBooks dependency
* Manual Monday inbound entry

---

## Systems of Record

| Data Type                | Source                               |
| ------------------------ | ------------------------------------ |
| Sales Velocity           | Amazon – Detail Page Sales & Traffic |
| FBA Inventory & Pipeline | Amazon – Restock Inventory           |
| SKU ↔ ASIN               | Google Sheets – Gross & Net          |
| Price                    | Google Sheets – Gross & Net          |
| Carton Quantity          | Google Sheets – Weights & Dims       |
| Calculations             | Python (pandas)                      |
| Output                   | Excel (presentation only)            |

---

## Project Status

* Business rules finalized
* Output schema frozen
* Source ownership defined
* Waiting only on final analyst confirmation for inbound selection preference

---

## Next Steps

1. Set up Amazon SP-API credentials
2. Set up Google Sheets service account access
3. Implement extract → transform → load pipeline in Python
4. Validate outputs against current analyst workflow
5. Prepare for Phase 2 (Monthly P&L)

---

This README is the **single source of truth** for the Weekly Amazon Summary Automation project.
