
-- Step-by-Step Breakdown of the Solution


-- We began by creating two key tables:

-- schema_history: This stores the current state of table schemas, which acts as a baseline for detecting changes.
-- ddl_history_log: This table logs any DDL changes made to the table structures, recording details such as column changes, data types, and timestamps.


CREATE OR REPLACE TABLE ddl_history_log
( id NUMBER AUTOINCREMENT PRIMARY KEY,
table_name STRING,
operation_type STRING,
column_name STRING,
old_data_type STRING,
new_data_type STRING,
change_timestamp TIMESTAMP_LTZ,
changed_by STRING
);

CREATE OR REPLACE TABLE schema_history AS
SELECT
table_name,
column_name,
data_type
FROM information_schema.columns
WHERE table_catalog = 'Provide_DB' and table_Schema ='Provide_Schema' ;



-- Stored Procedure:

CREATE OR REPLACE PROCEDURE monitor_ddl_history(db_name STRING, schema_name STRING, table_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Construct queries dynamically based on input parameters
var query_old = `SELECT column_name, data_type, table_name FROM ${DB_NAME}.${SCHEMA_NAME}.
schema_history WHERE table_name = '${TABLE_NAME}' ORDER BY table_name, column_name`;
var query_new = `SELECT column_name, data_type
                FROM ${DB_NAME}.information_schema.columns
                WHERE table_schema = '${SCHEMA_NAME}' AND table_name = '${TABLE_NAME}'
                ORDER BY column_name`;
// Execute the queries
var stmt_old = snowflake.createStatement({sqlText: query_old});
var stmt_new = snowflake.createStatement({sqlText: query_new});

var result_old = stmt_old.execute();
var result_new = stmt_new.execute();

var old_columns = {};
var new_columns = {};

// Populate old table structure
while (result_old.next()) {
    old_columns[result_old.getColumnValue (1)] = result_old.getColumnValue(2);
}

//return JSON.stringify (old_columns);
// A dict created like {"CRID": "TEXT", "CUST_CREATED": "DATE", "CUST_ID":"TEXT", "CUST_LOCATION": "TEXT", "TEXT", "LOCATION": "TEXT", "TOTAL_OUTSTANDING_AMT": "NUMBER"}

// Populate new table structure
while (result_new.next()) {
    new_columns[result_new.getColumnValue(1)] = result_new.getColumnValue(2);
}

// Check for new columns
for (var col in new_columns) {
    if (!(col in old_columns)) {
    var insert_add_column = `
        INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.ddl_history_log (table_name, operation_type,
        column_name, new_data_type, change_timestamp, changed_by)
        VALUES ('${TABLE_NAME}', 'ADD COLUMN', '${col}', '${new_columns[col]}',
        CURRENT_TIMESTAMP(), CURRENT_USER()) `;

var stmt_insert_add_column = snowflake.createStatement ({sqlText: insert_add_column});
stmt_insert_add_column.execute();
    }
}

// Check for removed or modified columns
for (var col in old_columns) {
    if (!(col in new_columns)) {
    var insert_drop_column = `
        INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.ddl_history_log(table_name, operation_type,
        column_name, old_data_type, change_timestamp, changed_by)
        VALUES ('${TABLE_NAME}', 'DROP COLUMN', '${col}', '${old_columns[col]}',
        CURRENT_TIMESTAMP(), CURRENT_USER()) `;
var stmt_insert_drop_column= snowflake.createStatement({sqlText: insert_drop_column});
stmt_insert_drop_column.execute();
}
else if (old_columns[col] != new_columns[col]) {
var insert_modify_column = `
    INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.ddl_history_log (table_name, operation_type,
    column_name, old_data_type, new_data_type, change_timestamp, changed_by)
    VALUES ('${TABLE_NAME}', 'MODIFY COLUMN', '${col}', '${old_columns[col]}', '${new_columns[col]}',
    CURRENT_TIMESTAMP(), CURRENT_USER())  `;

var stmt_insert_modify_column= snowflake.createStatement ({sqlText: insert_modify_column});
stmt_insert_modify_column.execute();
    }
}

var delete_snapshot = `
DELETE FROM ${DB_NAME}.${SCHEMA_NAME}.schema_history
WHERE table_name = '${TABLE_NAME}' `;

var stmt_delete_snapshot = snowflake.createStatement ({sqlText: delete_snapshot});
stmt_delete_snapshot.execute();

var insert_snapshot = `
    INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.schema_history (table_name, column_name, data_type)
    SELECT table_name, column_name, data_type FROM ${DB_NAME}.information_schema.columns
    WHERE table_schema = '${SCHEMA_NAME}'
    AND table_name = '${TABLE_NAME}' `;

var stmt_insert_snapshot = snowflake.createStatement({sqlText: insert_snapshot});
stmt_insert_snapshot.execute();

return 'DDL Changes Tracked and Schema Snapshot Updated Successfully!';
$$;




-- DDL Change proc
-- Execute the Stored procedure:

call monitor_ddl_history('Provide_DB', 'Provide_Schema', 'TEST');

call monitor_ddl_history('AUTO_COPY', 'DLINK', 'CUSTOMER');


SELECT * from ddl_history_log;