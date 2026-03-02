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

## 4. High-Level Architecture

```
Current Inventory
        |
        | Join on Part_Number
        v
Normalized GBOM
        |
        | Match on Platform + Three_CEID + Tool_Type + Process_Node
        v
Auto Tool List (Active Tools Only)
        |
        v
Executive & Drilldown Outputs
```

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
- `Tool_Type`  
- `Process_Node`  

| Field | Definition |
|--------|------------|
| Platform | Tool platform family (e.g., Telus, Tactras) |
| Three_CEID | First three letters of CEID |
| Tool_Type | Tool subtype classification |
| Process_Node | Numeric process node (e.g., 1274) |
| Qty_Per_Tool | Number of this part used per tool |

---

### 5.3 Auto Tool List (`cur_auto_tool_list`)

**Purpose:** Represents Intel's installed tool base.

**Grain:** One row per:

- `ENTITY`  
- `Process_Node`  

**Active tool definition:**

```
install_sts NOT IN ('Bagged', 'Not Installed')
```

This excludes:
- Bagged tools  
- Tools that are not installed in the fab  

---

### 5.4 Installed Base Aggregation (`mart_installed_base_by_config`)

**Purpose:** Pre-aggregates active tool counts for efficient joins.

**Grain:** One row per:

- `Platform`  
- `Three_CEID`  
- `Tool_Type`  
- `Process_Node`  

**Measures:**

- `Active_Tool_Count` (distinct count of active `ENTITY`)

---

## 6. Join Logic

### Step 1  
Join Inventory → GBOM on:

```
Part_Number
```

### Step 2  
Join GBOM → Installed Base on:

```
Platform
Three_CEID
Tool_Type
Process_Node
```

This determines whether a part supports any active tools.

---

## 7. Final Outputs

---

### 7.1 Output 1 — Executive Table

**Grain:** One row per `Part_Number`.

**Includes:**

Inventory metrics:
- `Qty`
- `Total_Amount`
- `Age`
- `MOU12`
- `MOU36`

Applicability rollups (comma lists):
- Platforms
- Three_CEIDs
- Tool_Types
- Process_Nodes

Installed base:
- `Active_Tool_Count_Total`
- `Active_Installed_Flag` (Y/N)

Disposition:
- `Disposition_Category`
- `Confidence_Level`

---

### 7.2 Output 2 — Drilldown Table

**Grain:** One row per:

- `Part_Number`
- `Platform`
- `Three_CEID`
- `Tool_Type`
- `Process_Node`

**Includes:**

- `Qty`
- `Total_Amount`
- `Qty_Per_Tool`
- `Active_Tool_Count`

Including `Qty` and `Total_Amount` allows prioritization of the highest financial exposure parts.

---

## 8. Disposition Logic

### High Confidence — Sell to Intel
- Part exists in GBOM  
- At least one matching active tool configuration exists  

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
| Tool_Type | Sub-type classification |
| Process_Node | Process configuration |
| Qty_Per_Tool | Units per tool |

---

### Auto Tool List

| Field | Meaning |
|--------|----------|
| ENTITY | Tool identifier |
| install_sts | Installation status |
| Location | Fab location |
| Process_Node | Process configuration |

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
│ (Filtered for Active Only)  │
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
- Clear grain definitions  
- No Excel-based logic  
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
