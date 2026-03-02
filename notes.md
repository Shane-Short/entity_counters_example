<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Inventory Installed Base Analysis - Project Technical Design & Business Documentation</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; line-height: 1.5; margin: 32px; color: #111; }
    h1, h2, h3 { line-height: 1.2; }
    code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    pre { background: #f6f8fa; padding: 12px; border-radius: 8px; overflow: auto; }
    table { border-collapse: collapse; width: 100%; margin: 12px 0; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
    th { background: #f2f2f2; }
    hr { border: 0; border-top: 1px solid #e5e5e5; margin: 24px 0; }
  </style>
</head>
<body>
  <h1>Inventory Installed Base Analysis</h1>
  <h2>Project Technical Design &amp; Business Documentation</h2>

  <hr />

  <h2>1. Objective</h2>
  <p>Leadership identified millions of dollars of parts sitting in warehouse inventory.</p>
  <p>The goal of this project is to determine:</p>
  <blockquote>
    <p><strong>Which inventory parts support active Intel tools, and which do not?</strong></p>
  </blockquote>
  <p>This enables us to:</p>
  <ul>
    <li><p>Sell parts back to Intel</p></li>
    <li><p>Reposition parts to other customers</p></li>
    <li><p>Identify obsolete inventory</p></li>
    <li><p>Reduce warehouse footprint</p></li>
    <li><p>Recover capital</p></li>
  </ul>

  <hr />

  <h2>2. Scope &amp; Supplier Handling</h2>
  <p>We will <strong>load all supplier codes</strong> from the inventory file into SQL.</p>
  <p>We will <strong>not</strong> filter <code>Supplier_Code</code> during raw ingestion.</p>
  <p>Instead:</p>
  <ul>
    <li><p>When we join Inventory &rarr; GBOM on <code>Part_Number</code></p></li>
    <li><p>Any part that does not exist in GBOM will naturally drop from relevance</p></li>
    <li><p>Final reporting can optionally filter to <code>Supplier_Code = 'ES'</code> if needed</p></li>
  </ul>
  <h3>Why this approach?</h3>
  <ul>
    <li><p>Prevents accidental data loss</p></li>
    <li><p>Keeps raw ingestion neutral and reproducible</p></li>
    <li><p>Makes the pipeline reusable</p></li>
    <li><p>Allows future expansion beyond ES inventory</p></li>
  </ul>

  <hr />

  <h2>3. Data Sources</h2>
  <p>We are using three primary datasets:</p>
  <ol>
    <li><p><strong>Current Inventory</strong> (<code>current_inventory.xlsb</code>)</p></li>
    <li><p><strong>GBOM</strong> (<code>GBOM.xlsx</code>)</p></li>
    <li><p><strong>Auto Tool List</strong> (<code>Auto_Tool_List.csv</code>)</p></li>
  </ol>

  <hr />

  <h2>4. High-Level Architecture</h2>
  <pre><code>Current Inventory
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
Executive &amp; Drilldown Outputs</code></pre>

  <hr />

  <h2>5. Data Model</h2>

  <h3>5.1 Inventory Table (<code>cur_inventory</code>)</h3>
  <p><strong>Purpose:</strong> Represents all warehouse inventory.</p>
  <p><strong>Grain:</strong> One row per:</p>
  <ul>
    <li><p><code>Region_Name</code></p></li>
    <li><p><code>Supplier_Code</code></p></li>
    <li><p><code>StorageLocation_Description</code></p></li>
    <li><p><code>Part_Number</code></p></li>
  </ul>

  <p><strong>Key Fields:</strong></p>
  <table>
    <thead>
      <tr><th>Field</th><th>Definition</th></tr>
    </thead>
    <tbody>
      <tr><td>Part_Number</td><td>Unique identifier of the part</td></tr>
      <tr><td>Qty</td><td>Quantity currently stored</td></tr>
      <tr><td>Total_Amount</td><td>Total dollar value</td></tr>
      <tr><td>Age</td><td>Time in storage</td></tr>
      <tr><td>MOU12</td><td>Months of use over last 12 months</td></tr>
      <tr><td>MOU36</td><td>Months of use over last 36 months</td></tr>
      <tr><td>ABC_LOCAL</td><td>Inventory priority classification</td></tr>
      <tr><td>NIS Machine Code</td><td>Internal tool classification</td></tr>
    </tbody>
  </table>

  <h3>5.2 GBOM Applicability (<code>cur_gbom_applicability</code>)</h3>
  <p><strong>Purpose:</strong> Defines which tool configurations each part supports.</p>
  <p><strong>Why normalize?</strong> GBOM is structured as a wide matrix with process nodes as columns. This is not join-friendly. We convert it to a row-based structure.</p>
  <p><strong>Grain:</strong> One row per:</p>
  <ul>
    <li><p><code>Part_Number</code></p></li>
    <li><p><code>Platform</code></p></li>
    <li><p><code>Three_CEID</code></p></li>
    <li><p><code>Tool_Type</code></p></li>
    <li><p><code>Process_Node</code></p></li>
  </ul>

  <table>
    <thead>
      <tr><th>Field</th><th>Definition</th></tr>
    </thead>
    <tbody>
      <tr><td>Platform</td><td>Tool platform family (e.g., Telus, Tactras)</td></tr>
      <tr><td>Three_CEID</td><td>First three letters of CEID</td></tr>
      <tr><td>Tool_Type</td><td>Tool subtype classification</td></tr>
      <tr><td>Process_Node</td><td>Numeric process node (e.g., 1274)</td></tr>
      <tr><td>Qty_Per_Tool</td><td>Number of this part used per tool</td></tr>
    </tbody>
  </table>

  <h3>5.3 Auto Tool List (<code>cur_auto_tool_list</code>)</h3>
  <p><strong>Purpose:</strong> Represents Intel's installed tool base.</p>
  <p><strong>Grain:</strong> One row per <code>ENTITY</code> + <code>Process_Node</code>.</p>
  <p><strong>Active tool definition:</strong> A tool is considered active if:</p>
  <pre><code>install_sts NOT IN ('Bagged', 'Not Installed')</code></pre>
  <p>This excludes bagged tools and tools that are not installed in the fab.</p>

  <h3>5.4 Installed Base Aggregation (<code>mart_installed_base_by_config</code>)</h3>
  <p><strong>Purpose:</strong> Pre-aggregates active tool counts for efficient joins.</p>
  <p><strong>Grain:</strong> One row per <code>Platform</code> + <code>Three_CEID</code> + <code>Tool_Type</code> + <code>Process_Node</code>.</p>
  <p><strong>Measures:</strong></p>
  <ul>
    <li><p><code>Active_Tool_Count</code> (distinct count of active <code>ENTITY</code>)</p></li>
  </ul>

  <hr />

  <h2>6. Join Logic</h2>
  <h3>Step 1</h3>
  <p>Join Inventory &rarr; GBOM on:</p>
  <pre><code>Part_Number</code></pre>

  <h3>Step 2</h3>
  <p>Join GBOM &rarr; Installed Base on:</p>
  <pre><code>Platform
Three_CEID
Tool_Type
Process_Node</code></pre>
  <p>This determines whether a part supports any active tools.</p>

  <hr />

  <h2>7. Final Outputs</h2>

  <h3>7.1 Output 1 &mdash; Executive Table</h3>
  <p><strong>Grain:</strong> One row per <code>Part_Number</code>.</p>
  <p><strong>Includes:</strong></p>
  <ul>
    <li><p>Inventory metrics: <code>Qty</code>, <code>Total_Amount</code>, <code>Age</code>, <code>MOU12</code>, <code>MOU36</code></p></li>
    <li><p>Applicability rollups (comma lists): Platforms, Three_CEIDs, Tool_Types, Process_Nodes</p></li>
    <li><p>Installed base: <code>Active_Tool_Count_Total</code>, <code>Active_Installed_Flag</code> (Y/N)</p></li>
    <li><p>Disposition: <code>Disposition_Category</code>, <code>Confidence_Level</code></p></li>
  </ul>

  <h3>7.2 Output 2 &mdash; Drilldown Table</h3>
  <p><strong>Grain:</strong> One row per <code>Part_Number</code> + <code>Platform</code> + <code>Three_CEID</code> + <code>Tool_Type</code> + <code>Process_Node</code>.</p>
  <p><strong>Includes:</strong> <code>Qty</code>, <code>Total_Amount</code>, <code>Qty_Per_Tool</code>, <code>Active_Tool_Count</code>.</p>
  <p>Including <code>Qty</code> and <code>Total_Amount</code> here allows prioritization of the highest financial exposure parts.</p>

  <hr />

  <h2>8. Disposition Logic</h2>
  <h3>High Confidence &mdash; Sell to Intel</h3>
  <ul>
    <li><p>Part exists in GBOM</p></li>
    <li><p>At least one matching active tool configuration exists</p></li>
  </ul>

  <h3>Medium Confidence &mdash; Investigate</h3>
  <ul>
    <li><p>Part exists in GBOM</p></li>
    <li><p>No matching active tools found</p></li>
  </ul>

  <h3>Low Confidence &mdash; Divest / Scrap Candidate</h3>
  <ul>
    <li><p>No GBOM match found</p></li>
  </ul>

  <hr />

  <h2>9. Full Field Definitions</h2>

  <h3>Inventory</h3>
  <table>
    <thead><tr><th>Field</th><th>Meaning</th></tr></thead>
    <tbody>
      <tr><td>Age</td><td>Time since part entered storage</td></tr>
      <tr><td>MOU12</td><td>Months used in last 12 months</td></tr>
      <tr><td>MOU36</td><td>Months used in last 36 months</td></tr>
      <tr><td>ABC_LOCAL</td><td>Inventory prioritization code</td></tr>
      <tr><td>NIS Machine Code</td><td>Internal classification</td></tr>
    </tbody>
  </table>

  <h3>GBOM</h3>
  <table>
    <thead><tr><th>Field</th><th>Meaning</th></tr></thead>
    <tbody>
      <tr><td>Platform</td><td>Tool family</td></tr>
      <tr><td>Three_CEID</td><td>CEID grouping</td></tr>
      <tr><td>Tool_Type</td><td>Sub-type classification</td></tr>
      <tr><td>Process_Node</td><td>Process configuration</td></tr>
      <tr><td>Qty_Per_Tool</td><td>Units per tool</td></tr>
    </tbody>
  </table>

  <h3>Auto Tool List</h3>
  <table>
    <thead><tr><th>Field</th><th>Meaning</th></tr></thead>
    <tbody>
      <tr><td>ENTITY</td><td>Tool identifier</td></tr>
      <tr><td>install_sts</td><td>Installation status</td></tr>
      <tr><td>Location</td><td>Fab location</td></tr>
      <tr><td>Process_Node</td><td>Process configuration</td></tr>
    </tbody>
  </table>

  <hr />

  <h2>10. Technical Data Flow</h2>
  <pre><code>┌─────────────────────────────┐
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
└─────────────────────────────┘</code></pre>

  <hr />

  <h2>11. Why This Design Is Simple</h2>
  <ul>
    <li><p>All raw files stored in SQL</p></li>
    <li><p>GBOM normalized once</p></li>
    <li><p>Active tool logic centralized</p></li>
    <li><p>Clear grain definitions</p></li>
    <li><p>No Excel-based logic</p></li>
    <li><p>Fully reproducible</p></li>
    <li><p>Easy for another developer to resume</p></li>
  </ul>

  <hr />

  <h2>12. What This Does Not Attempt</h2>
  <ul>
    <li><p>Demand forecasting</p></li>
    <li><p>Shipping cost optimization</p></li>
    <li><p>Time-series trend analysis</p></li>
    <li><p>Financial modeling beyond inventory value</p></li>
  </ul>
  <p>This strictly answers:</p>
  <blockquote>
    <p><strong>Does this inventory support active Intel tools today?</strong></p>
  </blockquote>
</body>
</html>
