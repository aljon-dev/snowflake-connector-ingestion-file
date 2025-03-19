const snowflake = require('snowflake-sdk');


// Connetor snowflake 

const connection = snowflake.createConnection({
    account: ' Account name ',
    username: 'username of your account',
    password: 'Password of you account ',
    role: 'ACCOUNTADMIN',
    database: 'Name of Database ',
    schema: 'PUBLIC',
    warehouse: 'COMPUTE_WH',
});


// Execution Query  you can use ES6 async/await syntax to make the code more readable and easier to maintain.


// like this  

/*  const executeQuery = async (connection, sqlText) =>{

    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sqlText,
            complete: (err, stmt, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({ stmt, rows });
                }
            }
        });
    });
    
 })

    */

async function executeQuery(connection, sqlText) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sqlText,
            complete: (err, stmt, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({ stmt, rows });
                }
            }
        });
    });
}

// Ingesting a CSV file into Snowflake

async function ingestCSV() {
    try {
        // Confirming the connection to Snowflake
        await connection.connectAsync();
        console.log('Connected to Snowflake successfully!');


        await executeQuery(connection, `CREATE OR REPLACE STAGE my_csv_stage`);
        console.log('Stage created successfully');

        //Execute the query to create a file format that will be used to load the CSV file into the table.
        await executeQuery(connection, `
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        `);
        console.log('File format created successfully');

        //  const filePath = 'C:/Users/Meow_Dev/Downloads/SampleData.csv'; either way works
            
        const filePath = 'C:\\Users\\Meow_Dev\\Downloads\\SampleData.csv';
        const putQuery = `PUT file://${filePath.replace(/\\/g, '/')} @my_csv_stage`;

        // Execute the query to upload the CSV file to the stage.
        await executeQuery(connection, putQuery);

        console.log('File uploaded to stage successfully');



        // Extracting the file name from the file path
        const fileName = filePath.split(/[\/\\]/).pop();

        // Execute the query to infer the schema of the CSV file.
        const inferSchemaQuery = `
            SELECT COLUMN_NAME, TYPE
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION=>'@my_csv_stage/${fileName}',
                    FILE_FORMAT=>'my_csv_format'
                )
            )
        `;


         // Execute the query to infer the schema of the CSV file.
        const schemaResult = await executeQuery(connection, inferSchemaQuery);

        if (!schemaResult.rows || schemaResult.rows.length === 0) {
            throw new Error('Schema inference failed. Please check your CSV file.');
        }

        // Create a table with the inferred schema
        const columnDefinitions = schemaResult.rows
            .map(row => `"${row.COLUMN_NAME}" ${row.TYPE}`)
            .join(', ');


        // Execute the query to create a table with the inferred schema.    
        const createTableQuery = `CREATE OR REPLACE TABLE MY_CSV_TABLE (${columnDefinitions})`;
        await executeQuery(connection, createTableQuery);
        console.log('Table created successfully');

         // Execute the query to copy the data from the CSV file into the table.
        const copyQuery = `
            COPY INTO MY_CSV_TABLE
            FROM @my_csv_stage/${fileName}
            FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        `;


        // Execute the query to copy the data from the CSV file into the table.
        await executeQuery(connection, copyQuery);
        console.log('Data loaded into table successfully');

        // Execute a sample query to verify the data ingestion.
        const result = await executeQuery(connection, `SELECT * FROM MY_CSV_TABLE LIMIT 5`);
        console.log('Sample data from the newly created table:', result.rows);
    } catch (err) {
        // Error Message
        console.error('Error during CSV ingestion:', err);
    } finally {
            // if finally do the needs it would destroy the connection
        if (connection) {
            connection.destroy();
        }
    }
}


ingestCSV();
