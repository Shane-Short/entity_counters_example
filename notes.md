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

---

## 3. Data Sources

We are using three primary datasets:

1. **Current Inventory** (`current_inventory.xlsb`)  
2. **GBOM** (`GBOM.xlsx`)  
3. **Auto Tool List** (`Auto_Tool_List.csv`)  

---

## 4. High-Level Architecture (Phase 1)

```
Current Inventory
        |
        | Join on Part_Number
        v
Normalized GBOM
        |
        | Match on Platform + Three_CEID + Process_Node
        v
Auto Tool List (Active Tools Only)
        |
        v
Executive & Drilldown Outputs
```

> NOTE: `Tool_Type` is included for reporting context but is NOT used as a join key in Phase 1 due to mismatch between GBOM subtype granularity and Auto Tool List data.

---

## 5. Data Model

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
|--------|------------|
| Part_Number | Unique identifier of the part |
| Qty | Quantity currently stored |
| Total_Amount | Total dollar value |
| Age | Time in storage |
| MOU12 | Months of use over last 12 months |
| MOU36 | Months of use over last 36 months |
| ABC_LOCAL | Inventory priority classification |
| NIS Machine Code | Internal tool classification |

---

### 5.2 GBOM Applicability (`cur_gbom_applicability`)

**Purpose:** Defines which tool configurations each part supports.

**Why normalize?**  
GBOM is structured as a wide matrix with process nodes as columns.  
This is not join-friendly. We convert it to a row-based structure.

**Grain:** One row per:

- `Part_Number`  
- `Platform`  
- `Three_CEID`  
- `Tool_Type` (for reporting only in Phase 1)  
- `Process_Node`  

| Field | Definition |
|--------|------------|
| Platform | Tool platform family |
| Three_CEID | First three letters of CEID |
| Tool_Type | Tool subtype classification (not used for join in Phase 1) |
| Process_Node | Numeric process node (e.g., 1274) |
| Qty_Per_Tool | Number of this part used per tool |

---

### 5.3 Auto Tool List (`cur_auto_tool_list`)

**Purpose:** Represents Intel's installed tool base.

**Grain:** One row per:

- `ENTITY`  
- `Process_Node`  

**Included Columns (Phase 1):**

- `PF` (renamed to `Platform`)
- `Three_CEID` (directly sourced — not recalculated)
- `Process_Node`
- `install_sts`
- `Location`

> NOTE: Auto Tool List does NOT currently include GBOM-level `Tool_Type` granularity (e.g., RLSA-BB). Therefore, Tool_Type is not used in Phase 1 joins.

---

### Active Tool Definition

```
install_sts NOT IN ('Bagged', 'Not Installed')
```

This excludes:

- Bagged tools  
- Tools not installed in the fab  

---

### 5.4 Installed Base Aggregation (`mart_installed_base_by_config`)

**Purpose:** Pre-aggregates active tool counts for efficient joins.

**Phase 1 Grain:** One row per:

- `Platform`
- `Three_CEID`
- `Process_Node`

> Tool_Type is excluded from aggregation grain in Phase 1.

---

### Critical Counting Logic (Prevents Overcounting)

The Auto Tool List contains multiple rows per tool (`ENTITY`) — one per `Process_Node`.

If we counted rows directly, a single tool with 12 process nodes would incorrectly count as 12 tools.

To prevent this:

**All tool counts must use:**

```
COUNT(DISTINCT ENTITY)
```

We never use raw row counts.

---

### Installed Base Metrics

#### 1. `Active_Tool_Count_Config`

Distinct count of active tools matching:

- Platform  
- Three_CEID  
- Process_Node  

**Calculation:**

```
COUNT(DISTINCT ENTITY)
GROUP BY Platform, Three_CEID, Process_Node
```

This answers:
> How many active tools exist for this configuration?

---

#### 2. `Active_Tool_Count_Unique`

Distinct count of active tools across all configurations a part supports.

**Calculation (after joining GBOM → ATL):**

```
COUNT(DISTINCT ENTITY)
GROUP BY Part_Number
```

This answers:
> How many unique active tools does this part support overall?

---

## 6. Join Logic (Phase 1)

### Step 1  
Join Inventory → GBOM on:

```
Part_Number
```

### Step 2  
Join GBOM → Active Installed Base on:

```
Platform
Three_CEID
Process_Node
```

> Tool_Type is excluded from join keys in Phase 1.

All tool counts use `COUNT(DISTINCT ENTITY)`.

---

## 7. Final Outputs

---

## 7.1 Output 1 — Executive Table

**Grain:** One row per `Part_Number`

---

### Inventory Metrics

| Column | Definition | Calculation |
|---------|------------|-------------|
| Qty_Total | Total on-hand quantity | SUM(Qty) |
| Total_Amount_USD | Total dollar exposure | SUM(Total_Amount) |
| MOU12 | Months used (12m) | MAX or AVG (documented choice) |
| MOU36 | Months used (36m) | MAX or AVG (documented choice) |

---

### Applicability Rollups

| Column | Definition | Calculation |
|---------|------------|-------------|
| Platforms | Platforms supported | STRING_AGG(DISTINCT Platform) |
| Three_CEIDs | CEID groups supported | STRING_AGG(DISTINCT Three_CEID) |
| Tool_Types | Tool types supported (reporting only) | STRING_AGG(DISTINCT Tool_Type) |
| Process_Nodes | Process nodes supported | STRING_AGG(DISTINCT Process_Node) |

---

### Installed Base Metrics

| Column | Definition | Calculation |
|---------|------------|-------------|
| Active_Tool_Count_Config_Total | Sum of config-level counts | SUM(Active_Tool_Count_Config) |
| Active_Tool_Count_Unique | Distinct active tools overall | COUNT(DISTINCT ENTITY) |
| Active_Installed_Flag | Indicates support of active tools | CASE WHEN Unique_Count > 0 THEN 'Y' ELSE 'N' |

---

### Disposition Fields

| Column | Logic |
|---------|-------|
| Disposition_Category | Sell / Investigate / Divest |
| Confidence_Level | High / Medium / Low |

Logic:

- Sell to Intel → GBOM match AND Active_Installed_Flag = Y  
- Investigate → GBOM match AND Active_Installed_Flag = N  
- Divest/Scrap → No GBOM match  

---

## 7.2 Output 2 — Drilldown Table

**Grain:** One row per:

- `Part_Number`
- `Platform`
- `Three_CEID`
- `Process_Node`
- `Tool_Type` (reporting only)

---

### Columns

| Column | Definition | Calculation |
|---------|------------|-------------|
| Qty_Total | Total inventory qty | SUM(Qty) |
| Total_Amount_USD | Total exposure | SUM(Total_Amount) |
| Qty_Per_Tool | Units required per tool | From GBOM |
| Active_Tool_Count_Config | Distinct active tools for config | COUNT(DISTINCT ENTITY) |

---

## 8. Disposition Logic

### High Confidence — Sell to Intel
- Part exists in GBOM  
- At least one matching active tool exists  

### Medium Confidence — Investigate
- Part exists in GBOM  
- No matching active tools found  

### Low Confidence — Divest / Scrap Candidate
- No GBOM match found  

---

## 9. Full Field Definitions

### Inventory

| Field | Meaning |
|--------|----------|
| Age | Time since part entered storage |
| MOU12 | Months used in last 12 months |
| MOU36 | Months used in last 36 months |
| ABC_LOCAL | Inventory prioritization code |
| NIS Machine Code | Internal classification |

---

### GBOM

| Field | Meaning |
|--------|----------|
| Platform | Tool family |
| Three_CEID | CEID grouping |
| Tool_Type | Sub-type classification (not used in Phase 1 joins) |
| Process_Node | Process configuration |
| Qty_Per_Tool | Units per tool |

---

### Auto Tool List

| Field | Meaning |
|--------|----------|
| ENTITY | Unique tool identifier |
| install_sts | Installation status |
| Three_CEID | CEID grouping |
| PF (Platform) | Tool family |
| Process_Node | Process configuration |
| Location | Fab location |

---

## 10. Technical Data Flow

```
┌─────────────────────────────┐
│ Current Inventory           │
└───────────────┬─────────────┘
                │
                ▼
┌─────────────────────────────┐
│ GBOM (Normalized)           │
└───────────────┬─────────────┘
                │
                ▼
┌─────────────────────────────┐
│ Active Installed Base       │
│ (DISTINCT ENTITY Count)     │
└───────────────┬─────────────┘
                │
                ▼
┌─────────────────────────────┐
│ Executive & Drilldown Views │
└─────────────────────────────┘
```

---

## 11. Why This Design Is Simple

- All raw files stored in SQL  
- GBOM normalized once  
- Active tool logic centralized  
- DISTINCT-based counting prevents overcount  
- Clear grain definitions  
- Tool_Type deferred until proper mapping exists  
- Fully reproducible  
- Easy for another developer to resume  

---

## 12. What This Does Not Attempt

- Demand forecasting  
- Shipping cost optimization  
- Time-series trend analysis  
- Financial modeling beyond inventory value  

This strictly answers:

> **Does this inventory support active Intel tools today?**
