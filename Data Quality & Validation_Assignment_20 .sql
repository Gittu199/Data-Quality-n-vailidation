-- ============================================================
--   DATA QUALITY AND VALIDATION IN ETL
--   Assignment – Question & Answer Sheet
--   File  : Data Quality & Validation_Assignment_20.sql
-- ============================================================


-- ============================================================
-- SECTION 1 : THEORY QUESTIONS (Q1 – Q6)
-- ============================================================

/*
========================================================================
QUESTION 1 :
  Define Data Quality in the context of ETL pipelines.
  Why is it more than just data cleaning?
========================================================================

ANSWER :

Data Quality in ETL refers to the degree to which data is accurate,
complete, consistent, timely, valid, and unique as it moves through the
Extract → Transform → Load pipeline.

It is MORE than just data cleaning because:

  1. ACCURACY   – Data must correctly represent real-world values
                  (e.g., Txn_Amount cannot be negative).

  2. COMPLETENESS – Critical fields must not be NULL or missing
                    (e.g., Quantity = NULL for Txn_ID 205 is a problem).

  3. CONSISTENCY  – Same entity should have the same representation
                    across all systems (e.g., "Mumbai" vs "mumbai").

  4. TIMELINESS   – Data must be available when needed; stale data
                    produces wrong analytics.

  5. UNIQUENESS   – No duplicate records should pollute aggregates.

  6. VALIDITY     – Values must conform to defined business rules
                    (e.g., date format YYYY-MM-DD, quantity > 0).

  7. INTEGRITY    – Relationships between tables must be honoured
                    (e.g., every Customer_ID in Sales must exist in
                    Customers_Master).

Data cleaning only handles fixing or removing bad values. Data quality
is a broader governance framework that prevents bad data from entering
the pipeline in the first place, enforces ongoing monitoring, and
ensures downstream consumers always receive trustworthy data.
*/


/*
========================================================================
QUESTION 2 :
  Explain why poor data quality leads to misleading dashboards
  and incorrect decisions.
========================================================================

ANSWER :

Dashboards and reports are only as reliable as the underlying data.
Poor data quality introduces errors at every stage:

  a) DUPLICATE RECORDS inflate KPIs.
     Example : Txn_IDs 201, 203, 208 are exact duplicates of
     (C101, P11, 2025-12-01, 4000). If all three are loaded, total
     revenue is overcounted by 8,000 (two extra rows × 4,000).

  b) NULL / MISSING VALUES skew averages.
     Txn_ID 205 has NULL Quantity; a SUM or AVG on Quantity will be
     wrong, and joins on that field will silently drop rows.

  c) INVALID REFERENCES produce incomplete reports.
     Customer_IDs C105 and C106 do not exist in Customers_Master.
     Any dashboard filtering by valid customers will silently omit
     those transactions or, worse, show them under "Unknown".

  d) NULL DATES break time-series charts.
     Txn_ID 207 has NULL Txn_Date; it cannot be placed on a timeline,
     corrupting trend lines.

  e) PLACEHOLDER VALUES ("N/A", "NULL" as text) cause wrong
     aggregations and filter results.

Consequence chain:
  Bad data → wrong metric → wrong insight → wrong business decision
  (e.g., overstocked inventory, missed revenue targets, wrong customer
  segmentation).
*/


/*
========================================================================
QUESTION 3 :
  What is duplicate data? Explain three causes in ETL pipelines.
========================================================================

ANSWER :

Duplicate data means two or more records that represent the same
real-world event or entity.

In the dataset, Txn_IDs 201, 203, and 208 are duplicates because
all four business-key columns are identical:
  Customer_ID = C101, Product_ID = P11,
  Txn_Date = 2025-12-01, Txn_Amount = 4000.

THREE COMMON CAUSES IN ETL PIPELINES :

  1. RE-PROCESSING WITHOUT DEDUPLICATION
     When a pipeline re-runs after a failure (e.g., network timeout),
     the same source records are extracted and loaded again without
     checking whether they already exist in the target table.

  2. MULTIPLE SOURCE SYSTEMS
     The same transaction may be recorded in two upstream systems
     (e.g., POS system + ERP system). If both feeds are ingested
     without a reconciliation step, the same sale appears twice.

  3. LACK OF IDEMPOTENCY / NO UPSERT LOGIC
     When the load step uses plain INSERT instead of INSERT … ON
     CONFLICT (upsert), every pipeline run appends rows regardless
     of whether identical rows already exist, accumulating duplicates
     over time.
*/


/*
========================================================================
QUESTION 4 :
  Differentiate between exact, partial, and fuzzy duplicates.
========================================================================

ANSWER :

  ┌─────────────┬──────────────────────────────────────────────────────┐
  │ Type        │ Description & Example from dataset                   │
  ├─────────────┼──────────────────────────────────────────────────────┤
  │ EXACT       │ Every column value is identical.                     │
  │ DUPLICATE   │ Txn_IDs 201, 203, 208 share the same Customer_ID,   │
  │             │ Product_ID, Quantity, Txn_Amount, Txn_Date, City.   │
  │             │ Only Txn_ID differs (surrogate key assigned by ETL). │
  ├─────────────┼──────────────────────────────────────────────────────┤
  │ PARTIAL     │ Key business columns match but some attributes       │
  │ DUPLICATE   │ differ.                                              │
  │             │ Example: same Customer + Product + Date but          │
  │             │ Txn_Amount differs → could be a price correction or  │
  │             │ a data entry error.                                  │
  ├─────────────┼──────────────────────────────────────────────────────┤
  │ FUZZY       │ Records refer to the same entity but differ due to   │
  │ DUPLICATE   │ typos, abbreviations, or formatting variations.      │
  │             │ Example: "Rahul Mehta" vs "R. Mehta" vs "rahul mehta"│
  │             │ all represent the same person.                       │
  │             │ Detected using similarity algorithms (Levenshtein,   │
  │             │ Soundex, Jaro-Winkler).                              │
  └─────────────┴──────────────────────────────────────────────────────┘
*/


/*
========================================================================
QUESTION 5 :
  Why should data validation be performed during transformation
  rather than after loading?
========================================================================

ANSWER :

Validating during the TRANSFORM stage (before loading) is the correct
approach for the following reasons:

  1. COST OF CORRECTION IS LOWER
     Fixing bad data in a staging/transform layer is far cheaper than
     correcting it in a production data warehouse or data mart where
     downstream reports may already have consumed it.

  2. PREVENTS DATA POLLUTION
     Null amounts (Txn_ID 206), null dates (Txn_ID 207), and unknown
     customers (C105, C106) should be flagged and quarantined in a
     "reject" table during transformation, not silently loaded.

  3. REFERENTIAL INTEGRITY CAN BE CHECKED EARLY
     Verifying that every Customer_ID exists in Customers_Master during
     transformation prevents orphan rows from reaching the target DB.

  4. PERFORMANCE
     It is faster to discard or fix a bad row in memory (transform
     engine) than to DELETE / UPDATE it later in a large target table
     with indexes and constraints.

  5. AUDITABILITY
     Transformation-stage validation produces a clean audit trail of
     rejected records with reasons, enabling source-system teams to
     fix the issue at the origin.

  6. AVOIDS CONSTRAINT VIOLATIONS
     If the target table has NOT NULL or FOREIGN KEY constraints, bad
     records will cause the entire load to fail. Validating upfront
     allows selective rejection rather than a full pipeline abort.
*/


/*
========================================================================
QUESTION 6 :
  Explain how business rules help in validating data accuracy.
  Give an example.
========================================================================

ANSWER :

Business rules are domain-specific constraints that define what
"correct" data looks like beyond mere technical formatting.

HOW THEY HELP :
  • They translate real-world logic into checkable conditions.
  • They catch values that are technically valid (non-null, correct
    type) but semantically wrong.
  • They form the basis for automated validation checks in ETL.

EXAMPLE FROM THE DATASET :

  Business Rule : "Quantity must be a positive integer greater than 0."

  Violation : Txn_ID 205 → Quantity = NULL.
              A NULL quantity means we cannot calculate revenue
              (Quantity × Unit_Price). This row should be routed to a
              reject table with error code "MISSING_QUANTITY".

  Additional business-rule checks applicable to this dataset:
    a) Txn_Amount > 0          → Txn_ID 206 violates this (NULL amount).
    b) Txn_Date IS NOT NULL    → Txn_ID 207 violates this.
    c) Customer_Name != 'N/A'  → Txn_ID 206 violates this.
    d) Customer_ID must exist
       in Customers_Master     → C105, C106 violate referential integrity.

  By encoding these rules as ETL validation steps, bad records are
  intercepted, logged, and either corrected or quarantined before they
  distort dashboards.
*/


-- ============================================================
-- SECTION 2 : DATASET SETUP
--             (Create & populate tables used in Q7 and Q8)
-- ============================================================

DROP TABLE IF EXISTS Sales_Transactions;
DROP TABLE IF EXISTS Customers_Master;

-- Customers_Master table
CREATE TABLE Customers_Master (
    CustomerID   VARCHAR(10)  PRIMARY KEY,
    CustomerName VARCHAR(100) NOT NULL,
    City         VARCHAR(50)
);

INSERT INTO Customers_Master (CustomerID, CustomerName, City) VALUES
('C101', 'Rahul Mehta',  'Mumbai'),
('C102', 'Anjali Rao',   'Bengaluru'),
('C103', 'Suresh Iyer',  'Chennai'),
('C104', 'Neha Singh',   'Delhi');

-- Sales_Transactions table
CREATE TABLE Sales_Transactions (
    Txn_ID        INT          PRIMARY KEY,
    Customer_ID   VARCHAR(10),
    Customer_Name VARCHAR(100),
    Product_ID    VARCHAR(10),
    Quantity      INT,
    Txn_Amount    DECIMAL(10,2),
    Txn_Date      DATE,
    City          VARCHAR(50)
);

INSERT INTO Sales_Transactions
    (Txn_ID, Customer_ID, Customer_Name, Product_ID, Quantity, Txn_Amount, Txn_Date, City)
VALUES
(201, 'C101', 'Rahul Mehta', 'P11', 2,    4000.00, '2025-12-01', 'Mumbai'),
(202, 'C102', 'Anjali Rao',  'P12', 1,    1500.00, '2025-12-01', 'Bengaluru'),
(203, 'C101', 'Rahul Mehta', 'P11', 2,    4000.00, '2025-12-01', 'Mumbai'),
(204, 'C103', 'Suresh Iyer', 'P13', 3,    6000.00, '2025-12-02', 'Chennai'),
(205, 'C104', 'Neha Singh',  'P14', NULL, 2500.00, '2025-12-02', 'Delhi'),
(206, 'C105', 'N/A',         'P15', 1,    NULL,    '2025-12-03', 'Pune'),
(207, 'C106', 'Amit Verma',  'P16', 1,    1800.00, NULL,         'Pune'),
(208, 'C101', 'Rahul Mehta', 'P11', 2,    4000.00, '2025-12-01', 'Mumbai');


-- ============================================================
-- QUESTION 7 :
--   Write an SQL query on Sales_Transactions to list all
--   duplicate keys and their counts using the business key
--   (Customer_ID + Product_ID + Txn_Date + Txn_Amount).
-- ============================================================

/*
APPROACH :
  Group by all four business-key columns.
  Use HAVING COUNT(*) > 1 to retain only groups that appear more
  than once – those are the duplicates.
  The COUNT column shows exactly how many times each key appears.
*/

SELECT
    Customer_ID,
    Product_ID,
    Txn_Date,
    Txn_Amount,
    COUNT(*)                  AS Duplicate_Count,
    GROUP_CONCAT(Txn_ID)      AS Duplicate_Txn_IDs   -- lists all Txn_IDs sharing that key
FROM
    Sales_Transactions
GROUP BY
    Customer_ID,
    Product_ID,
    Txn_Date,
    Txn_Amount
HAVING
    COUNT(*) > 1
ORDER BY
    Duplicate_Count DESC;

/*
EXPECTED OUTPUT :
  ┌─────────────┬────────────┬────────────┬────────────┬─────────────────┬──────────────────────┐
  │ Customer_ID │ Product_ID │  Txn_Date  │ Txn_Amount │ Duplicate_Count │  Duplicate_Txn_IDs   │
  ├─────────────┼────────────┼────────────┼────────────┼─────────────────┼──────────────────────┤
  │    C101     │    P11     │ 2025-12-01 │  4000.00   │        3        │      201,203,208     │
  └─────────────┴────────────┴────────────┴────────────┴─────────────────┴──────────────────────┘

  Explanation :
    Business key (C101, P11, 2025-12-01, 4000) appears in Txn_IDs 201, 203, and 208.
    The pipeline should keep only one (e.g., the lowest Txn_ID = 201)
    and reject/delete the other two as exact duplicates.
*/


-- ============================================================
-- QUESTION 8 :
--   Enforcing Referential Integrity
--   Identify Sales_Transactions.Customer_ID values that violate
--   referential integrity when joined with Customers_Master,
--   and write a query to detect such violations.
-- ============================================================

/*
APPROACH :
  Use a LEFT JOIN from Sales_Transactions to Customers_Master on
  Customer_ID.  Any Sales row whose Customer_ID has NO match in
  Customers_Master will have NULL in the Customers_Master columns
  after the join.
  Filter with WHERE cm.CustomerID IS NULL to surface violations.
*/

SELECT
    st.Txn_ID,
    st.Customer_ID      AS Violating_Customer_ID,
    st.Customer_Name    AS Provided_Name,
    st.City             AS Provided_City,
    'Customer_ID not found in Customers_Master' AS Violation_Reason
FROM
    Sales_Transactions  st
LEFT JOIN
    Customers_Master    cm
    ON  st.Customer_ID = cm.CustomerID
WHERE
    cm.CustomerID IS NULL
ORDER BY
    st.Txn_ID;

/*
EXPECTED OUTPUT :
  ┌────────┬──────────────────────┬───────────────┬───────────────┬────────────────────────────────────────────┐
  │ Txn_ID │ Violating_Customer_ID│ Provided_Name │ Provided_City │           Violation_Reason                 │
  ├────────┼──────────────────────┼───────────────┼───────────────┼────────────────────────────────────────────┤
  │  206   │        C105          │      N/A      │     Pune      │ Customer_ID not found in Customers_Master  │
  │  207   │        C106          │  Amit Verma   │     Pune      │ Customer_ID not found in Customers_Master  │
  └────────┴──────────────────────┴───────────────┴───────────────┴────────────────────────────────────────────┘

  Explanation :
    • C105 (Txn_ID 206) – Customer_Name is "N/A" and has no master record.
      This row should be quarantined until the source system provides a
      valid customer registration.
    • C106 (Txn_ID 207) – Amit Verma exists in the transaction data but
      is absent from Customers_Master. The transaction cannot be linked
      to a verified customer profile.
    • C101, C102, C103, C104 all have matching records and pass the
      referential integrity check.
*/


-- ============================================================
-- END OF ASSIGNMENT
-- Data Quality and Validation in ETL
-- ============================================================
