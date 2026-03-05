# Inventory Installed Base Analysis
## Project Technical Design & Business Documentation
---

## 1. Objective

Leadership identified millions of dollars of parts sitting in warehouse inventory.

The goal of this project is to determine:

> **Which inventory parts support active Intel tools, and which do not?**

This enables us to:

- Sell parts back to Intel
- Reposition parts to other customers
- Identify obsolete inventory
- Reduce warehouse footprint
- Recover capital

---

## 2. Scope & Supplier Handling

We will **load all supplier codes** from the inventory file into SQL.

We will **not** filter `Supplier_Code` during raw ingestion.

Instead:

- When we join Inventory → GBOM on `Part_Number`
- Any part that does not exist in GBOM will naturally drop from relevance
- Final reporting can optionally filter to `Supplier_Code = 'ES'` if needed

### Why this approach?

- Prevents accidental data loss
- Keeps raw ingestion neutral and reproducible
- Makes the pipeline reusable
- Allows future expansion beyond ES inventory

> **Clarification:** `Supplier_Code` here is the **BU** (ex: `ES` = Etch, `CT` = Clean Track), not `CEID` and not `Three_CEID`.

---

## 3. Data Sources

We are using three primary datasets:

1. **Current Inventory** (`current_inventory.xlsb`)
2. **GBOM** (`GBOM.xlsx`)
3. **Auto Tool List** (`Auto_Tool_List.csv`)

---

## 4. High-Level Architecture

**Phase 1 (current):** tool applicability and active installed base counts using:

- `Platform + Three_CEID + Process_Node`
- **MOM/platform-level tools only** (no chamber entities)

Current Inventory
|
| Join on Part_Number
v
Normalized GBOM (Applicability)
|
| Match on Platform + Three_CEID + Process_Node
v
Auto Tool List (Active, MOM-only + Tool Age)
|
v
Leadership (Executive) + Drilldown Outputs

> **Note:** We are **not** using `Tool_Type` as a join key in Phase 1.

---

## 5. Data Model

### What “cur” and “mart” mean

- `cur_*` = **curated**: cleaned/standardized tables or views that are business-ready (stable columns, consistent types).
- `mart_*` = **data mart**: reporting-focused tables or views (often aggregated for speed, stable grain).

---

### 5.1 Inventory Table (`cur_inventory`)

**Purpose:** Represents all warehouse inventory.

**Grain:** One row per:

- `Region_Name`
- `Supplier_Code`
- `StorageLocation_Description`
- `Part_Number`

#### Key Fields

| Field | Definition |
|---|---|
| Part_Number | Unique identifier of the part |
| Qty | Quantity currently stored |
| Total_Amount | Total dollar value (extended value) |
| Age | Time in storage (bucketed in the source file) |
| MOU12 | Months of usage in the last 12 months |
| MOU36 | Months of usage in the last 36 months |
| ABC_LOCAL | Inventory priority classification |
| NIS Machine Code | Internal tool classification |
| Supplier_Code | BU code (ES/CT/…) |

> **MOU definition:** MOU = **Months Of Usage** (not quantity used). It indicates whether the part saw usage within the month window.

---

### 5.2 GBOM Applicability (`cur_gbom_applicability`)

**Purpose:** Defines which tool configurations each part supports.

**Why normalize?**  
GBOM is structured as a wide matrix with process nodes as columns. This is not join-friendly. We convert it to a row-based structure.

**Grain:** One row per:

- `Part_Number`
- `Platform`
- `Three_CEID`
- `Tool_Type` *(kept for reference, not used for Phase 1 join)*
- `Process_Node`

| Field | Definition |
|---|---|
| Platform | Tool platform family (e.g., Telus, Tactras) |
| Three_CEID | CEID grouping key (3 letters) |
| Tool_Type | Tool subtype classification (GBOM detail; not used in Phase 1 join) |
| Process_Node | Numeric process node (e.g., 1274) |
| Qty_Per_Tool | Quantity required **per MOM/platform tool** for that configuration |

#### Critical clarification: what `Qty_Per_Tool` actually means

In this context, `Qty_Per_Tool` is best interpreted as a **tool-level requirement** that already reflects chamber usage.  
Example: if `Qty_Per_Tool = 4`, it often means **4 chambers on that tool use the part** (so 4 per full tool).

Because of this, we must avoid counting chamber-level tool rows (e.g., `_PM1`, `_PM2`, etc.) in installed base counts, otherwise downstream calculations can **double-count** chamber usage.

---

### 5.3 Auto Tool List (`cur_auto_tool_list`)

**Purpose:** Represents Intel's installed tool base (including warranty dates used for tool age).

**Source reality:** The raw Auto Tool List contains:

- MOM/platform-level tools (e.g., `ABC123`)
- Chamber-level tools (e.g., `ABC123_PM1`, `ABC123_PM2`, ...)

For this project, we produce a curated view that supports **MOM-only analytics**.

#### 5.3.1 Curated MOM-level view (`cur_auto_tool_list_mom`)

**Purpose:** Provides “tool” records at the MOM/platform entity level for counting and age bucketing.

**MOM-only filter rule:**
- Keep only rows where `ENTITY` **does not** contain an underscore `_`

Equivalent:
- `CHARINDEX('_', ENTITY) = 0` (SQL Server-style)

**Grain:** One row per:
- `ENTITY` (MOM tool)
- `Process_Node`

A single MOM tool can still appear multiple times (one row per process node).

#### Included Columns (Phase 1 matching)

- `Platform` (from source `PF`)
- `Three_CEID` (**directly sourced — not recalculated**)
- `Process_Node`
- `install_sts`
- `Location` / `Fab`
- `Warranty_Start` *(needed for tool age bucketing)*

#### Active tool definition
install_sts NOT IN (‘Bagged’, ‘Not Installed’)

---

## 6. Critical Counting Rules (No Overcounting)

This section is the “guard rail” that prevents inflated active tool counts and inflated part demand.

### 6.1 We only count MOM/platform entities (not chambers)

- We **exclude** chamber entities such as `ABC123_PM1`, `ABC123_PM2`, etc.
- We **include** only the MOM/platform entity such as `ABC123`

Reason:
- `Qty_Per_Tool` already represents chamber usage at the tool level.
- Counting chambers and then also using `Qty_Per_Tool` would exaggerate demand.

### 6.2 Active tool counts must always be `COUNT(DISTINCT ENTITY)`

Because Auto Tool List has multiple rows per tool (one per process node), we **never count rows**.

All “active tool” metrics are based on:
COUNT(DISTINCT ENTITY)

…and are computed from `cur_auto_tool_list_mom` (MOM-only).

### 6.3 We do NOT sum the same tool across multiple process nodes

A tool may have many `Process_Node` rows. If you sum per-node tool counts naively, you can count the same tool multiple times.

Preventative design:
- Config-level counts are always `COUNT(DISTINCT ENTITY)` at the config grain.
- Any rollups must be defined carefully (either “per-config view” or “per-part overall distinct tool view”), never “sum of config counts” unless the sets are guaranteed disjoint.

### 6.4 How “parts needed” should be interpreted

- **Active tool count** = `COUNT(DISTINCT MOM ENTITY)` (MOM-only; not multiplied)
- **Estimated total parts needed for a config** (if desired) can be computed as:
  Estimated_Parts_Needed_Config = Active_Tool_Count_Config * Qty_Per_Tool

This is valid because:
- `Qty_Per_Tool` already approximates “how many chambers on the tool use this part”
- and the installed base count is at the MOM tool level (not chamber level)

> Important: the “active tool count” itself is NOT multiplied. Only the optional “estimated parts needed” uses multiplication by `Qty_Per_Tool`.

### 6.5 Example: Preventing Tool Overcounting

It is important to understand how active tools are counted so that installed base metrics are not inflated.

A single tool may appear multiple times in the Auto Tool List because it can run multiple process nodes.

Example Auto Tool List rows:

ENTITY        Platform      Process_Node
ABC123        Tactras BX    1234
ABC123        Tactras BX    1232
ABC123        Tactras BX    1230

Even though the tool appears in three rows, it is still **one physical tool**.

For installed base calculations we therefore count **distinct MOM/platform entities only**, not rows.

Active tool counts are always calculated as:

COUNT(DISTINCT ENTITY)

This ensures that a single tool is never counted multiple times simply because it supports multiple process nodes.

In addition, chamber-level entities are excluded entirely. For example:

ABC123        → MOM / platform tool (included)
ABC123_PM1    → chamber (excluded)
ABC123_PM2    → chamber (excluded)

Chamber entities are excluded because `Qty_Per_Tool` in the GBOM already represents the number of chambers on the tool that use the part.

Counting chambers as tools would artificially inflate the installed base and exaggerate estimated part demand.

Therefore the installed base logic follows these rules:

1. Only MOM/platform tools are counted (ENTITY values without "_").
2. Only active tools are included (`install_sts NOT IN ('Bagged','Not Installed')`).
3. Tool counts always use `COUNT(DISTINCT ENTITY)`.
4. Tools are never summed across process nodes.

Example:

If tool `ABC123` exists under process nodes `1234`, `1232`, and `1230`, and a part has `Qty_Per_Tool = 4`, then:

Active tool count for that part = **1 tool**

Optional demand estimate for that configuration:

Estimated_Parts_Needed = Active_Tool_Count * Qty_Per_Tool  
Estimated_Parts_Needed = 1 * 4 = 4

### 6.6 Understanding Tool Counts vs Configuration Counts vs Demand Estimates

Installed base analysis involves three related but different concepts.  
Confusing these is the most common cause of incorrect part demand estimates.

This section clearly defines the difference.

---

#### 1. Tool Count (Physical Installed Base)

A **tool count** represents the number of physical MOM/platform tools installed.

Rules:

- Only MOM/platform entities are counted (ENTITY without "_").
- Chamber entities are excluded.
- Tools are counted using:

```
COUNT(DISTINCT ENTITY)
```

Example Auto Tool List rows:

| ENTITY | Platform | Process_Node |
|------|------|------|
| ABC123 | Tactras BX | 1234 |
| ABC123 | Tactras BX | 1232 |
| ABC123 | Tactras BX | 1230 |

Even though the tool appears three times, there is still **only one tool**.

Tool count = **1**

This is the **true installed base**.

---

#### 2. Configuration Count (Tool Capability)

A **configuration** represents a specific combination of:

```
Platform + Three_CEID + Process_Node
```

A single tool may support multiple configurations because it can run multiple process nodes.

Example:

| ENTITY | Platform | Three_CEID | Process_Node |
|------|------|------|------|
| ABC123 | Tactras BX | GTA | 1234 |
| ABC123 | Tactras BX | GTA | 1232 |
| ABC123 | Tactras BX | GTA | 1230 |

These represent **three configurations**, but still only **one tool**.

Configuration-level metrics are stored in:

```
mart_installed_base_by_config
```

Where:

```
Active_Tool_Count_Config = COUNT(DISTINCT ENTITY)
```

This count is **distinct per configuration**, not summed across configurations.

---

#### 3. Demand Estimation (Optional)

Demand estimation uses tool counts combined with GBOM applicability.

Formula:

```
Estimated_Parts_Needed_Config =
Active_Tool_Count_Config * Qty_Per_Tool
```

Where:

- `Active_Tool_Count_Config` = distinct MOM tools in that configuration
- `Qty_Per_Tool` = number of parts required per MOM tool

Example:

| Config | Active Tools | Qty_Per_Tool |
|------|------|------|
| Tactras BX / GTA / 1276 | 2 | 4 |

Estimated demand:

```
2 tools * 4 parts per tool = 8 parts
```

Important:

- **Tool counts are never multiplied by chamber counts**
- `Qty_Per_Tool` already represents chamber usage

---

#### 4. Why These Distinctions Matter

Incorrect approach (causes inflated demand):

```
SUM(Active_Tool_Count_Config)
```

This counts the same tool multiple times if it supports multiple process nodes.

Correct approach:

- Always use `COUNT(DISTINCT ENTITY)`
- Never sum configuration tool counts unless sets are guaranteed disjoint.

---

#### Summary

| Concept | Meaning | Calculation |
|------|------|------|
| Tool Count | Physical installed tools | `COUNT(DISTINCT ENTITY)` |
| Configuration Count | Tool capability by process node | `COUNT(DISTINCT ENTITY)` per config |
| Demand Estimate | Potential parts required | `Active_Tool_Count_Config * Qty_Per_Tool` |

Separating these concepts ensures:

- Accurate installed base counts
- No double-counting across process nodes
- Realistic inventory demand estimates

---

## 7. Installed Base Aggregation (`mart_installed_base_by_config`)

**Purpose:** Pre-aggregates active tool counts for efficient joins.

**Input:** `cur_auto_tool_list_mom` (MOM-only)

**Grain:** One row per:
- `Platform`
- `Three_CEID`
- `Process_Node`

**Measures:**
- `Active_Tool_Count_Config` = `COUNT(DISTINCT ENTITY)` for active MOM tools in that config

Optional (recommended) breakdown measures by **tool age bucket** (defined below):
- `Active_Tool_Count_0_1Y`
- `Active_Tool_Count_1_3Y`
- `Active_Tool_Count_3_5Y`
- `Active_Tool_Count_5P_Y`
- `Active_Tool_Count_UnknownAge`

---

## 8. Join Logic

### 8.1 Step 1 — Inventory → GBOM (Applicability)
Join on:
Part_Number

### 8.2 Step 2 — GBOM → Active Installed Base (Phase 1)
Join on:
Platform
Three_CEID
Process_Node

> **Why Tool_Type is not used (Phase 1):**  
Auto Tool List does not currently provide a Tool_Type at the same granularity as GBOM (example: GBOM may have `RLSA-BB` while source tool data may only say `RLSA`). Until a reliable mapping exists, Tool_Type is excluded from join logic.

---

## 9. Tool Age Calculation and Bucketing

**Purpose:** Break out active tools by tool age (0–1y, 1–3y, 3–5y, 5+ years) using warranty start.

**Source field:** `Warranty_Start` from `cur_auto_tool_list_mom`

**Tool_Age calculation (at run time):**
- `Tool_Age_Days = DATEDIFF(day, Warranty_Start, AsOf_Date)`
- `Tool_Age_Years = Tool_Age_Days / 365.25`

**AsOf_Date:**
- Default: pipeline run date (or a parameterized report “as-of” date)

**Buckets (recommended):**
- `0_1Y`   = `Tool_Age_Years < 1`
- `1_3Y`   = `Tool_Age_Years >= 1 AND < 3`
- `3_5Y`   = `Tool_Age_Years >= 3 AND < 5`
- `5P_Y`   = `Tool_Age_Years >= 5`
- `Unknown` = `Warranty_Start IS NULL`

**Counting rule:**  
Tool age bucket metrics are always distinct counts of MOM tools:
- `COUNT(DISTINCT ENTITY)` filtered to the bucket.

---

## 10. Final Outputs

### Recommendation: two deliverables (to satisfy both audiences)

1. **Leadership Product Table** (one row per part number; minimal but decision-ready)
2. **Detailed Drilldown Table** (one row per part + configuration; deep dive / traceability)

This resolves the feedback conflict:
- Leadership wants a small, usable “product”
- Engineers/analysts want traceability and drilldown

---

## 10.1 Output 1 — Leadership Product Table (Executive)

**Grain:** One row per `Part_Number`

### Columns and Definitions

| Column | Definition | How it’s calculated |
|---|---|---|
| Part_Number | Part identifier | From `cur_inventory` |
| Supplier_Code | BU code (ES/CT/…) | From `cur_inventory` |
| Amount_of_stock | Total on-hand quantity | `SUM(Qty)` over all inventory rows for the part |
| Total_Amount_USD | Total $ exposure | `SUM(Total_Amount)` over all inventory rows for the part |
| Cost_per_part | Average $ per unit | `Total_Amount_USD / NULLIF(Amount_of_stock,0)` |
| MOU12 | Months of usage (12m) | `MAX(MOU12)` |
| MOU36 | Months of usage (36m) | `MAX(MOU36)` |
| Amount_needed_per_tool | Representative qty per MOM tool | See rule below |
| Number_of_active_tools | Unique active MOM tools supported (all ages) | `COUNT(DISTINCT ENTITY)` after applicability join to `cur_auto_tool_list_mom` |
| Active_Tools_0_1Y | Active MOM tools age 0–1 years | distinct count filtered to bucket |
| Active_Tools_1_3Y | Active MOM tools age 1–3 years | distinct count filtered to bucket |
| Active_Tools_3_5Y | Active MOM tools age 3–5 years | distinct count filtered to bucket |
| Active_Tools_5P_Y | Active MOM tools age 5+ years | distinct count filtered to bucket |
| Active_Tools_UnknownAge | Active MOM tools missing Warranty_Start | distinct count filtered to bucket |
| Active_Installed_Flag | Y if supports any active tool | `CASE WHEN Number_of_active_tools > 0 THEN 'Y' ELSE 'N' END` |
| Disposition_Category | Sell / Investigate / Divest | See section 11 |
| Confidence_Level | High / Medium / Low | See section 11 |

#### Amount_needed_per_tool (how we pick one number)

GBOM can contain multiple `Qty_Per_Tool` values depending on configuration. For leadership, we need **one** representative number.

**Default recommendation (conservative):**
- `Amount_needed_per_tool = MAX(Qty_Per_Tool)` across all applicable GBOM configs for that part

---

## 10.2 Output 2 — Detailed Drilldown Table

**Grain:** One row per:
- `Part_Number`
- `Platform`
- `Three_CEID`
- `Process_Node`

### Columns and Definitions

| Column | Definition | How it’s calculated |
|---|---|---|
| Part_Number | Part identifier | From inventory / GBOM |
| Platform | Tool platform | From GBOM |
| Three_CEID | 3-letter CEID group | From GBOM |
| Process_Node | Process node | From GBOM |
| Tool_Type | GBOM tool subtype (reference only) | From GBOM |
| Qty_Per_Tool | Units required per MOM tool | From GBOM |
| Active_Tool_Count_Config | Active MOM tools for this config (all ages) | From `mart_installed_base_by_config` (`COUNT DISTINCT ENTITY`) |
| Active_Tools_0_1Y | Config-level active MOM tools 0–1 years | from mart |
| Active_Tools_1_3Y | Config-level active MOM tools 1–3 years | from mart |
| Active_Tools_3_5Y | Config-level active MOM tools 3–5 years | from mart |
| Active_Tools_5P_Y | Config-level active MOM tools 5+ years | from mart |
| Active_Tools_UnknownAge | Config-level active MOM tools unknown age | from mart |
| Qty_Total | Total inventory qty for the part | `SUM(Qty)` from inventory (repeated for context) |
| Total_Amount_USD | Total exposure for the part | `SUM(Total_Amount)` (repeated for context) |
| Cost_per_part | Average $ per unit | `Total_Amount_USD / NULLIF(Qty_Total,0)` |

Optional (if stakeholders want it):
- `Estimated_Parts_Needed_Config = Active_Tool_Count_Config * Qty_Per_Tool`

---

## 11. Disposition Logic

### High Confidence — Sell to Intel
- Part exists in GBOM
- At least one matching active MOM tool exists (`Number_of_active_tools > 0`)

### Medium Confidence — Investigate
- Part exists in GBOM
- No matching active MOM tools found (`Number_of_active_tools = 0`)

### Low Confidence — Divest / Scrap Candidate
- No GBOM match found (no applicability rows)

---

## 12. Example Data (for review & sanity checking)

These examples are intended to make review easy and allow a reviewer to “simulate” the joins.

### 12.1 Current Inventory (`cur_inventory`) — example rows (input)

| Region_Name | Supplier_Code | Part_Number | Part_Description | StorageLocation_Description | Age | Qty | Total_Amount_USD | MOU12 | MOU36 |
|---|---|---|---|---|---|---:|---:|---:|---:|
| TEL | ES | ES3D10-150813-V1 | SHIELD, DEPO QZ-U-FC2 SPO | Kashiwa2GDC_SPX | 4~5 Years | 558 | 2985272.10 | 4 | 6 |
| TEL | ES | ES2L10-152971-V1 | ELECTRODE, BOTTOMSMZP | Narita2_SPX | 8~+ Years | 67 | 1804039.32 | 0 | 2 |
| TEL | ES | ES3D10-250834-V1 | CEL, OX T10-75-C912 (COC-N) | Narita2_SPX | 2~3 Years | 960 | 1249737.60 | 1 | 3 |

> Note: a part can have multiple inventory rows if stored in multiple locations; totals are calculated via `SUM()` across those rows.

---

### 12.2 Normalized GBOM (`cur_gbom_applicability`) — example rows (output of normalization)

| Part_Number | Platform | Three_CEID | Tool_Type | Process_Node | Qty_Per_Tool |
|---|---|---|---|---:|---:|
| ES3D10-150813-V1 | Tactras | ANT | RLSA+RLSA-BB | 1274 | 4 |
| ES3D10-150813-V1 | Tactras | ANT | RLSA-BB | 1276 | 4 |
| ES3D10-150813-V1 | Tactras | ONT | DRM | 1276 | 4 |
| ES3D10-150813-V1 | Tactras | TAO | T4+DS-1 | 1274 | 4 |

---

### 12.3 Auto Tool List MOM-only (`cur_auto_tool_list_mom`) — example rows (input)

| ENTITY | PF (Platform) | Location | CEID | Three_CEID | Process_Node | install_sts | Warranty_Start |
|---|---|---|---|---|---:|---|---|
| GTA467 | Tactras BX | Portland | G8Aca | GTA | 1276 | Installed | 2019-11-14 |
| GTA511 | Tactras BX | Portland | GTAca | GTA | 1276 | Installed | 2022-04-04 |
| OXT514 | Telius | Portland | OXTcp | OXT | 1276 | Installed | 2021-06-15 |

> Chamber-level rows like `GTA467_PM1` are intentionally excluded from this curated MOM-only view.

---

### 12.4 Installed Base Mart (`mart_installed_base_by_config`) — example rows (output)

| Platform | Three_CEID | Process_Node | Active_Tool_Count_Config | Active_Tool_Count_0_1Y | Active_Tool_Count_1_3Y | Active_Tool_Count_3_5Y | Active_Tool_Count_5P_Y | Active_Tool_Count_UnknownAge |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Tactras BX | GTA | 1276 | 2 | 0 | 1 | 0 | 1 | 0 |
| Telius | OXT | 1276 | 1 | 0 | 0 | 1 | 0 | 0 |

---

### 12.5 Example End-to-End Walkthrough (Example A: “Sell” case)

#### A1) Inventory input
| Part_Number | Qty | Total_Amount_USD |
|---|---:|---:|
| ES3D10-150813-V1 | 558 | 2985272.10 |

#### A2) GBOM applicability output (subset)
| Part_Number | Platform | Three_CEID | Process_Node | Qty_Per_Tool |
|---|---|---|---:|---:|
| ES3D10-150813-V1 | Tactras BX | GTA | 1276 | 4 |

#### A3) Installed base config has active MOM tools
Assume `mart_installed_base_by_config` shows `Active_Tool_Count_Config = 2` for `(Tactras BX, GTA, 1276)`.

#### A4) Leadership output (expected)
| Part_Number | Amount_of_stock | Total_Amount_USD | Cost_per_part | Amount_needed_per_tool | Number_of_active_tools | Active_Installed_Flag | Disposition_Category | Confidence_Level |
|---|---:|---:|---:|---:|---:|---|---|---|
| ES3D10-150813-V1 | 558 | 2985272.10 | 5349.95 | 4 | 2 | Y | Sell | High |

Optional demand estimate (if requested):
- `Estimated_Parts_Needed_Config = 2 * 4 = 8` (for that config)

---

### 12.6 Example End-to-End Walkthrough (Example B: “Investigate” case)

#### B1) Inventory input (example)
| Part_Number | Qty | Total_Amount_USD |
|---|---:|---:|
| ESX-INVESTIGATE-EXAMPLE | 10 | 25000.00 |

#### B2) GBOM applicability exists (example)
| Part_Number | Platform | Three_CEID | Process_Node | Qty_Per_Tool |
|---|---|---|---:|---:|
| ESX-INVESTIGATE-EXAMPLE | Tactras BX | ANT | 1276 | 2 |

#### B3) Installed base has no active MOM tools for that config
| Platform | Three_CEID | Process_Node | Active_Tool_Count_Config |
|---|---|---:|---:|
| Tactras BX | ANT | 1276 | 0 |

#### B4) Leadership output (expected)
| Part_Number | Number_of_active_tools | Active_Installed_Flag | Disposition_Category | Confidence_Level |
|---|---:|---|---|---|
| ESX-INVESTIGATE-EXAMPLE | 0 | N | Investigate | Medium |

---

### 12.7 Example End-to-End Walkthrough (Example C: “Divest” case)

#### C1) Inventory input
| Part_Number | Qty | Total_Amount_USD |
|---|---:|---:|
| ESTW39-000039-13T | 1 | 368181.82 |

#### C2) No GBOM match
| Part_Number | GBOM_Match_Flag |
|---|---|
| ESTW39-000039-13T | N |

#### C3) Leadership output (expected)
| Part_Number | GBOM_Match_Flag | Disposition_Category | Confidence_Level |
|---|---|---|---|
| ESTW39-000039-13T | N | Divest | Low |

---

## 13. Why This Design Is Simple

- Raw files stored in SQL
- GBOM normalized once
- Active tool logic centralized
- **MOM-only counting prevents chamber inflation**
- DISTINCT-based counting prevents overcount across process nodes
- Tool age handled with an explainable bucket rule
- Clear grain definitions
- Fully reproducible
- Easy for another developer to resume

---

## 14. Next Steps

1. **Finalize “Amount_needed_per_tool” rule** for leadership table
    - Default: `MAX(Qty_Per_Tool)`; confirm with stakeholders.

2. **Confirm MOM-only rule is acceptable**
    - Default: filter `ENTITY` to exclude any value containing `_`.

3. **Confirm tool age buckets**
    - Default: 0–1, 1–3, 3–5, 5+ years + Unknown (missing Warranty_Start)

4. **Deliver Excel output with two sheets**
    - Sheet 1: `Leadership_Product_Table`
    - Sheet 2: `Drilldown_Table`

5. **(Optional Phase 2) Tool_Type mapping**
    - If stakeholders want Tool_Type-accurate joins, define and validate a mapping from source tool classification to GBOM Tool_Type granularity.

---
