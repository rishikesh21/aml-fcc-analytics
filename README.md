# Weekly Alert Pipeline Readme

This document outlines the structure and logic of the weekly alert pipeline, including the data linking process, the core alerting mechanism, the generated outputs, and a crucial data/configuration finding related to Mod3.

---

# Pipeline Overview

The pipeline integrates three primary input tables: **Models**, **Rules**, and **Hits**, to generate weekly customer alerts based on aggregated scores and defined thresholds.

## 1. Data Integration and Preprocessing

* **Models Table Usage**: Used to define the **alert threshold** for each unique combination of model and customer type (e.g., Mod1_INV is mapped to **Mod1** + **Individual**).
* **Rules Table Usage**: Provides the necessary mapping to assign a specific **model** and **score** to every rule hit, categorized by the correct customer type.
* **Hits Processing**:
    * Dates are parsed, and each hit is assigned to a specific **week ending Sunday**.
    * A **deduplication rule** is applied: only the first instance of a hit counts if it shares the same **customer**, **rule**, and **week**.

## 2. Alert Generation Logic

* **Weekly Score Aggregation**: For each unique **customer** and **model**, the deduped weekly scores are summed.
* **Threshold Comparison**: The total weekly score is compared against the corresponding model's alert threshold (obtained from the **Models** table).
* An **alert is triggered** if the summed weekly score meets or exceeds the threshold.

---

# Pipeline Outputs

The generated alerts are used to create three required output files, located in `data/output/`:

* `alerts_trends_overview.csv`: Provides insights into alert patterns and trends over time.
* `rule_hits_overview.csv`: Offers a detailed breakdown and statistics of which rules are contributing to hits.
* `general_statistics.csv`: Contains high-level summary statistics about the pipeline's weekly performance.

---

# Data/Configuration Sanity Check and Finding (Mod3)

A crucial sanity check was implemented to validate the viability of the current model configurations.

## Sanity Check Method

For every unique combination of **model** and **customer type**, the **maximum possible score** that could be accumulated from all relevant rules was calculated and directly compared against the model's defined **threshold**.

## Finding: Mod3 (Individual) Configuration Error

The sanity check flagged a critical configuration mistake:

| Model/Customer | Threshold | Max Possible Score (from Rules) | Status |
| :--- | :--- | :--- | :--- |
| **Mod3 (Individual)** | **30** | **20** | **Alert IMPOSSIBLE** |

Because the maximum potential score of **20** is less than the required threshold of **30**, **Mod3** can **never trigger a weekly alert** for **Individual** customers with the existing rule configuration. This requires a review of either the rule scores or the threshold.

---

# Files

* **Script**: `src/pipeline.py`
* **Outputs Directory**: `data/output/`
