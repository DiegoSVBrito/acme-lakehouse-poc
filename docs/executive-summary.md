# Executive Summary
## ACME Inc. — Sales & Margin Analytics POC

Author: Diego Brito · Pomerol
Reference Date: April 1, 2021 (data cutoff)  |  Document: June 2026

---

## Where We Started

ACME Inc. had no unified data infrastructure. Sales, product, customer, and operational data lived in flat files — CSV, XML, and Excel — with no governance, no lineage tracking, and no single source of truth. There was no pipeline, no data quality monitoring, and no model ready for analytics consumption. The data itself carried years of undocumented issues that had never been surfaced.

---

## What We Built

We designed and implemented a full Medallion Architecture on Microsoft Fabric Lakehouse, covering all phases of a modern data warehouse:

- **Bronze layer** — 11 source files ingested as 12 immutable Delta tables, with full lineage tracking and encoding fixes. Sources untouched.
- **Silver layer** — typed, cleaned, and enriched data with explicit null handling, geographic enrichment, known error corrections, and a quarantine framework for invalid records.
- **Gold layer** — a Constellation Star Schema with two line-level fact tables, one budget fact, and five conformed dimensions — fully ready for BI consumption via Direct Lake.
- **Data Quality layer** — 9 automated checks across orders, order details, and shipments, with results persisted in `gold_dq_issues` and available for governance dashboards.
- **Insights notebook** — 13 SQL-driven analyses covering all required business dimensions, bonus questions, and full visual output.

---

## Challenges

The data sources presented several structural problems that required deliberate engineering decisions rather than simple ingestion:

- **Windows-1252 encoding** in CSV files was causing silent character corruption in city and company names — fixed at the Bronze read layer.
- **DivisionID = 2 primary key violation** — two different business divisions shared the same ID in the source file, making customer segmentation unreliable. The duplicate was quarantined and Central America was assigned a new ID (6), derived programmatically from country-level geographic reference data.
- **ShipperIDs 4 and 5** referenced in 20% of orders had no master record in Shippers.csv. Unknown Member records were injected to preserve financial integrity without fabricating data.
- **TotalOrder field was zero for all 6,571 orders** in the source — the column existed but was never populated. Revenue was recomputed from Order_Details using the correct line-item formula.
- **Budget Excel file** used merged cells and wide format — required forward-fill logic and unpivoting to produce a usable long-format fact table.

---

## Key Findings

**Data Quality**

- 100% of product transactions carry a different unit price from the current catalog — confirmed SCD (Slowly Changing Dimension) behavior. Every sale preserved the historical transaction price, while the catalog has since been updated. This is expected and documented, not a pipeline error.
- The dominant DQ volume comes from two known, documented sources: legacy shipment dates and the SCD price pattern — not from structural integrity failures.
- `shipment_employee_mismatch` returned approximately 3,400 records where the employee who processed the shipment differs from the one who took the order. This directly impacts Sales Rep performance reporting and commission attribution.

**Business**

- Revenue is heavily concentrated in one division — high dependency risk.
- Clear seasonal patterns are visible in the monthly revenue heatmap — inventory planning opportunity.
- Two registered customers have never placed an order — low-cost activation opportunity.
- Products with zero stock and open orders exist in the current catalog — immediate supply chain alert needed.

---

## Legacy Problems We Cannot Fix

One issue falls entirely outside the scope of engineering intervention:

Shipment dates (2007–2012) predate all order dates (2016–2021). Every single delivery record is temporally impossible — shipments registered years before the corresponding orders were created. This is a legacy system migration artifact. The data is preserved in Bronze as received, flagged in Silver with a `_data_warning` column on every row, and quantified in the DQ layer. Delivery SLA is not measurable with the current shipment data. The path forward requires the logistics team to provide records from the current operational system — the pipeline is ready to receive and process them when available.

---

## Status

All eight deliverables specified in the project scope are complete. The Gold layer is connected to the semantic model via Direct Lake. Every business question — including all bonus items — is answered with SQL and visual output in the insights notebook. The solution is fully documented and ready for stakeholder walkthrough.

---

Diego Brito · Pomerol · June 2026
