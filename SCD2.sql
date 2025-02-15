CREATE OR REPLACE VIEW v_customer_changing_data AS (
    -- INSERT/FALSE: NEW ROW - Records that are new and should be inserted into the customer table
    WITH insert_row AS (
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME,
            EMAIL, 
            STREET, 
            CITY,
            STATE,
            COUNTRY,
            UPDATE_TIMESTAMP,
            'I' AS DML_TYPE -- 'I' for Insert operation
        FROM scd_demo.scd2.customer_table_changes_i
        WHERE METADATA$ACTION = 'INSERT' -- Filter for insert actions
        AND METADATA$ISUPDATE = 'FALSE' -- Ensure that the data is not an update, but a new record
    ),
    
    -- UPDATE: The entry that will be marked as 'current' in the SCD2 table
    update_row_insert AS (
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME,
            EMAIL, 
            STREET, 
            CITY,
            STATE,
            COUNTRY,
            UPDATE_TIMESTAMP,
            'U' AS DML_TYPE -- 'U' for Update operation
        FROM scd_demo.scd2.customer_table_changes_i
        WHERE METADATA$ACTION = 'INSERT' -- Filter for insert actions
        AND METADATA$ISUPDATE = 'TRUE' -- Mark as an update, for when existing data has changed
    ),
    
    -- DELETE: The entry that will be marked as 'not-current' in the SCD2 table
    update_row_delete AS (
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME,
            EMAIL, 
            STREET, 
            CITY,
            STATE,
            COUNTRY,
            UPDATE_TIMESTAMP,
            'D' AS DML_TYPE -- 'D' for Delete operation
        FROM scd_demo.scd2.customer_table_changes_i
        WHERE METADATA$ACTION = 'DELETE' -- Filter for delete actions
        AND METADATA$ISUPDATE = 'TRUE' -- Ensure it’s an update operation that represents a deletion
    ),
    
    -- Combining all the changes (INSERT, UPDATE, DELETE) into a single dataset
    joined_table_changes AS (
        SELECT * FROM insert_row
        UNION
        SELECT * FROM update_row_insert
        UNION 
        SELECT * FROM update_row_delete
    )
    
    -- Final selection and formatting of the data for the SCD2 merge logic
    SELECT  
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME,
        EMAIL, 
        STREET, 
        CITY,
        STATE,
        COUNTRY,
        UPDATE_TIMESTAMP AS start_time, -- Treat the update timestamp as the start time of the record
        LAG(UPDATE_TIMESTAMP) OVER(PARTITION BY CUSTOMER_ID ORDER BY UPDATE_TIMESTAMP DESC) AS end_time_raw, -- Using LAG to get the previous timestamp (end time)
        
        -- Set end time to '9999-12-31' if there is no previous record (i.e., first entry)
        CASE 
            WHEN end_time_raw IS NULL THEN '9999-12-31'::timestamp_ntz 
            ELSE end_time_raw 
        END AS end_time,
        
        -- Set 'is_current' flag to TRUE for the latest record, and FALSE for older ones
        CASE 
            WHEN end_time_raw IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        DML_TYPE -- To indicate whether the operation is 'Insert', 'Update', or 'Delete'
    
    FROM joined_table_changes
);

-- Query to view the contents of the created view
SELECT * FROM v_customer_changing_data;


-- Start of the MERGE statement to synchronize data from source to target
MERGE INTO customer_history ch
USING v_customer_changing_data ccd
    -- Condition to match records between the target table (customer_history) and source view (v_customer_changing_data)
    ON ch.customer_id = ccd.customer_id
    AND ch.start_time = ccd.start_time

-- When a matching record is found and the operation is a delete (D) in the source data (ccd)
WHEN MATCHED AND ccd.dml_type = 'D' THEN
    -- Update the matched record in the target table (customer_history) to mark it as historical
    UPDATE
    SET ch.end_time = ccd.end_time,  -- Set the end_time to the value from the source (ccd)
        ch.is_current = FALSE         -- Mark the record as not current (historical)
    
-- When no matching record is found (new record in the source data)
WHEN NOT MATCHED THEN
    -- Insert a new record into the customer_history table with data from the source (ccd)
    INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, START_TIME, END_TIME, IS_CURRENT)
    VALUES (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, ccd.street, ccd.city, ccd.state, ccd.country, ccd.start_time, ccd.end_time, ccd.is_current);
    -- Insert the customer data with the relevant details, including the start_time, end_time, and the flag for current data

-- Return a message indicating the operation has been completed
RETURN 'COMPLETED';
-- End of the MERGE operation
END;
