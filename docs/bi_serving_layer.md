# BI / Serving Layer — Two Approaches

The gold layer is deliberately consumable by **two independent BI front ends**.
This is a design choice, not redundancy: it demonstrates the trade-offs between
an external enterprise BI tool and the platform-native AI/BI layer, and it lets
the same governed metrics serve both.

Both approaches read the **same gold tables** — no gold logic changes between
them. The semantic/metric definitions are kept consistent so the two surfaces
report identical numbers.

---

## Approach 1 — Power BI (external enterprise BI)

The original serving layer. Power BI connects to the Databricks SQL warehouse,
imports/queries the gold marts, and presents three report pages:

1. Market overview — metro-level health and trend lines
2. County drill-down — which sub-markets are hot, cooling, or stressed
3. Affordability deep-dive — the narrative page

**Why keep it:** Power BI is the dominant enterprise BI tool and a core
existing strength. The semantic model (relationships, measures in DAX,
row-level formatting) showcases mature dimensional-modeling skills.

Artifact: `dashboard/market_intelligence.pbix`

---

## Approach 2 — Databricks AI/BI (platform-native)

A second serving layer built entirely inside Databricks, requiring no external
tool. Two components:

### 2a. Metric view (the semantic layer)
A Unity Catalog **metric view** defines the reusable measures and dimensions
once — months of supply, median sale price, price-to-income, affordability
index — so both the dashboard and Genie compute identical numbers. This is the
direct analogue of a Power BI semantic model / DAX measure set.

Defined in `notebooks/30_metric_view.py`.

### 2b. AI/BI Dashboard + Genie Space
- **AI/BI Dashboard**: low-code visuals over the metric view, with
  cross-filtering, built natively in the workspace.
- **Genie Space**: a conversational interface over the same metric view.
  Business users ask questions in natural language ("which RDU county had the
  fastest price growth last year?") and get governed answers — no SQL required.
  Sample questions and instructions are curated to improve answer quality.

**Why add it:** It proves platform-native fluency on the exact stack the role
centers on, and it reframes the semantic-modeling skill in Databricks terms.

---

## The interview story this enables

> "I served the same gold layer two ways. Power BI for the established
> enterprise-BI audience, and Databricks AI/BI — a metric view feeding both an
> AI/BI dashboard and a Genie space — for self-service inside the platform. I
> defined the metrics once as a governed semantic layer so both surfaces agree
> to the dollar. It let me compare an external BI tool against the native
> lakehouse BI experience directly."

## Availability note (Free Edition)

AI/BI Dashboards and Genie are available to Databricks SQL users with no extra
license; Free Edition includes serverless SQL, so both are usable here. Genie
quality depends on the metric view's semantics and curated sample questions —
treat that curation as part of the build, not an afterthought.
