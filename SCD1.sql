CREATE OR REPLACE PROCEDURE pdr_scd_demo()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
    -- Perform the MERGE operation between the 'customer' table and the 'customer_raw' staging table
    MERGE INTO customer c
    USING customer_raw cr
    ON c.customer_id = cr.customer_id

    -- Condition to detect if there's a difference between the existing customer data and the raw customer data
    WHEN MATCHED AND (
        c.customer_id <> cr.customer_id
        OR c.first_name <> cr.first_name
        OR c.last_name <> cr.last_name
        OR c.email <> cr.email
        OR c.street <> cr.street
        OR c.city <> cr.city
        OR c.state <> cr.state
        OR c.country <> cr.country
    )
    THEN
        -- If there are differences, update the customer table with the new data
        UPDATE SET 
            c.first_name = cr.first_name,
            c.last_name = cr.last_name,
            c.email = cr.email,
            c.street = cr.street,
            c.city = cr.city,
            c.state = cr.state,
            c.country = cr.country,
            c.update_timestamp = CURRENT_TIMESTAMP() -- Updating the timestamp of the last modification

    -- If no match is found, insert new data into the 'customer' table
    WHEN NOT MATCHED
    THEN
        INSERT (customer_id, first_name, last_name, email, street, city, state, country)
        VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);

    -- After the MERGE operation, truncate the 'customer_raw' staging table
    -- This ensures that the raw data is cleared for the next load
    TRUNCATE TABLE customer_raw;

    -- Return a success message indicating the procedure ran successfully
    RETURN 'Procedure executed successfully';
END;
$$;
