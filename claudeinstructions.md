**To:** Claude Development Team
**From:** Gemini CLI Co-Architect
**Date:** September 4, 2025
**Project:** DataMine V2
**Subject: TICKET-136: Standardize Primary Key and Final Validation**

---

### **1.0 Objective**

To finalize the pipeline's architecture, we must standardize the primary key for the `05_candidate_events` table to use the `raw_event_hash_id`, which is the common key used in the rest of our database.

Your objective is to modify the table schema, update the Polars test script to use this new key, clear out old data, re-run the test, and perform a final validation against our labeled training data.

### **2.0 Implementation Plan**

This is a four-part plan. Please execute the steps in order.

#### **Part 1: Update Database Schema**

*   **Assigned Persona:** `LEAD_DATA_ENGINEER`
*   **Action:** Modify the `05_candidate_events` table to use `raw_event_hash_id` as its primary key.
*   **Commands:** Connect to the `datamine_v2_db` as the `ahs_user` and run the following SQL commands:
    ```sql
    -- First, remove the old, auto-incrementing primary key
    ALTER TABLE "05_candidate_events" DROP CONSTRAINT "05_candidate_events_pkey";
    ALTER TABLE "05_candidate_events" DROP COLUMN "event_id";

    -- Add the new primary key column
    ALTER TABLE "05_candidate_events" ADD COLUMN "raw_event_hash_id" TEXT NOT NULL;

    -- Set the new primary key
    ALTER TABLE "05_candidate_events" ADD PRIMARY KEY ("raw_event_hash_id");

    -- Clear any old data from previous tests
    TRUNCATE TABLE "05_candidate_events";
    ```

#### **Part 2: Modify the Polars Test Script**

*   **Assigned Persona:** `FULL_STACK_DEVELOPER`
*   **Action:** Modify the `05_cpd_polars_single_test.py` script to work with the new schema.
*   **Logic:**
    1.  **Update Data Extraction:** Modify the `COPY` query to also extract the `raw_event_hash_id` from the `04_primary_feature_table`.
    2.  **Update Polars Processing:** Carry the `raw_event_hash_id` through the Polars DataFrame alongside the `timestamp` and signal data.
    3.  **Update Data Insertion:** Modify the `bulk_insert_change_points` function. The data it inserts should now be a list of tuples, each containing the `raw_event_hash_id` that corresponds to the detected change point's timestamp.

#### **Part 3: Execute the Test**

*   **Assigned Persona:** `LEAD_DATA_ENGINEER`
*   **Action:** Run the modified `05_cpd_polars_single_test.py` script one final time to populate the `05_candidate_events` table with the new, correctly keyed data.

#### **Part 4: Final Analysis**

*   **Assigned Persona:** `LEAD_DATA_ENGINEER`
*   **Action:** Connect to the database and run the provided proximity join query to compare the new candidate events against the labeled data in `02.1_labeled_training_data_loaddump`.
*   **Query:**
    ```sql
    WITH ground_truth AS (
        SELECT timestamp, ml_event_label AS event_label
        FROM "02.1_labeled_training_data_loaddump"
        WHERE device_id = 'lake-605-10-0050' AND timestamp::date = '2025-07-30'
    ),
    candidate_events AS (
        SELECT timestamp_start
        FROM "05_candidate_events"
        WHERE device_id = 'lake-605-10-0050'
          AND timestamp_start::date = '2025-07-30'
    )
    SELECT
        gt.timestamp AS ground_truth_event_time,
        gt.event_label,
        (SELECT ce.timestamp_start
         FROM candidate_events ce
         WHERE ce.timestamp_start BETWEEN gt.timestamp - INTERVAL '2 minutes' AND gt.timestamp + INTERVAL '2 minutes'
         ORDER BY ABS(EXTRACT(EPOCH FROM (gt.timestamp - ce.timestamp_start)))
         LIMIT 1) AS closest_detected_event_time,
        (SELECT ABS(EXTRACT(EPOCH FROM (gt.timestamp - ce.timestamp_start)))
         FROM candidate_events ce
         WHERE ce.timestamp_start BETWEEN gt.timestamp - INTERVAL '2 minutes' AND gt.timestamp + INTERVAL '2 minutes'
         ORDER BY ABS(EXTRACT(EPOCH FROM (gt.timestamp - ce.timestamp_start)))
         LIMIT 1) AS time_difference_seconds
    FROM ground_truth gt
    ORDER BY gt.timestamp;
    ```

### **3.0 Deliverable**

*   Provide the full output of the final analysis query from Part 4. This will give us the definitive answer on what our algorithm is detecting.

---

This is the final validation step. Please proceed.