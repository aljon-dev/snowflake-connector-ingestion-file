const snowflake = require('snowflake-sdk');

// Connect to Snowflake
const connection = snowflake.createConnection({
    account: '',
    username: '',
    password: '',
    role: 'ACCOUNTADMIN',
    database: '',
    schema: 'PUBLIC',
    warehouse: 'COMPUTE_WH',
});

// Execute query function using Promises
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

// Ingest CSV function
async function ingestCSV() {
    try {
        // Clean up old objects
        try {
            await executeQuery(connection, `DROP TABLE IF EXISTS WEATHER_DATA`);
            await executeQuery(connection, `DROP STAGE IF EXISTS weather_data_stage`);
            await executeQuery(connection, `DROP FILE FORMAT IF EXISTS weather_data_format`);
            console.log('✅ Cleaned up existing objects');
        } catch (cleanupErr) {
            console.error('⚠️ Cleanup error (non-fatal):', cleanupErr);
        }

        // Connect to Snowflake
        await connection.connectAsync();
        console.log('✅ Connected to Snowflake successfully!');

        // Create a Snowflake stage
        await executeQuery(connection, `CREATE OR REPLACE STAGE weather_data_stage`);
        console.log('✅ Stage created successfully');

     
        await executeQuery(connection, `
           CREATE OR REPLACE FILE FORMAT weather_data_format
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('NULL', 'null', '', 'None')  -- Add 'None' to your NULL_IF list
        `);
        console.log('✅ File format created successfully');

        // Path to CSV file
        const filePath = 'C:\\Users\\Meow_Dev\\Downloads\\SampleData.csv';
        const fileName = filePath.split(/[\/\\]/).pop();
        const putQuery = `PUT file://${filePath.replace(/\\/g, '/')} @weather_data_stage AUTO_COMPRESS=FALSE`;

        // Upload CSV to Snowflake
        await executeQuery(connection, putQuery);
        console.log('✅ File uploaded to stage successfully');

        // First infer the schema to get data types
        const inferSchemaQuery = `
            SELECT COLUMN_NAME, TYPE
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION=>'@weather_data_stage/${fileName}',
                    FILE_FORMAT=>'weather_data_format'
                )
            )
        `;
        const schemaResult = await executeQuery(connection, inferSchemaQuery);

        if (!schemaResult.rows || schemaResult.rows.length === 0) {
            throw new Error('❌ Schema inference failed. Please check your CSV format.');
        }

        console.log(`✅ Inferred schema for ${schemaResult.rows.length} columns`);

        // Create table with all 169 columns using your provided headers
        const createTableQuery = `
            CREATE OR REPLACE TABLE WEATHER_DATA (
                COUNTRY_CODE VARCHAR,
                ADMIN_CODE VARCHAR,
                CITY_NAME VARCHAR,
                LATITUDE FLOAT,
                LONGITUDE FLOAT,
                DATE TIMESTAMP_NTZ,
                CATEGORY_FLIGHT_PREDOMINANT VARCHAR,
                CLOUD_BASE_HEIGHT_24HR_DEP FLOAT,
                CLOUD_BASE_HEIGHT_AVG FLOAT,
                CLOUD_BASE_HEIGHT_MAX FLOAT,
                CLOUD_BASE_HEIGHT_MIN FLOAT,
                CLOUD_COVER_24HR_DEP FLOAT,
                CLOUD_COVER_AVG FLOAT,
                CLOUD_COVER_MAX FLOAT,
                CLOUD_COVER_MIN FLOAT,
                DEGREE_DAYS_COOLING FLOAT,
                DEGREE_DAYS_EFFECTIVE FLOAT,
                DEGREE_DAYS_FREEZING FLOAT,
                DEGREE_DAYS_GROWING FLOAT,
                DEGREE_DAYS_HEATING FLOAT,
                EVAPOTRANSPIRATION_LWE_TOTAL FLOAT,
                HAS_FREEZING_RAIN BOOLEAN,
                FREEZING_RAIN_LWE_TOTAL FLOAT,
                FREEZING_RAIN_LWE_RATE_AVG FLOAT,
                FREEZING_RAIN_LWE_RATE_MAX FLOAT,
                FREEZING_RAIN_LWE_RATE_MIN FLOAT,
                HUMIDITY_RELATIVE_24HR_DEP FLOAT,
                HUMIDITY_RELATIVE_AVG FLOAT,
                HUMIDITY_RELATIVE_MAX FLOAT,
                HUMIDITY_RELATIVE_MIN FLOAT,
                HAS_ICE BOOLEAN,
                ICE_LWE_TOTAL FLOAT,
                ICE_LWE_RATE_AVG FLOAT,
                ICE_LWE_RATE_MAX FLOAT,
                ICE_LWE_RATE_MIN FLOAT,
                INDEX_UV_24HR_DEP FLOAT,
                INDEX_UV_AVG FLOAT,
                INDEX_UV_MAX FLOAT,
                INDEX_UV_MIN FLOAT,
                MINUTES_OF_FREEZING_RAIN_TOTAL INTEGER,
                MINUTES_OF_ICE_TOTAL INTEGER,
                MINUTES_OF_PRECIPITATION_TOTAL INTEGER,
                MINUTES_OF_RAIN_TOTAL INTEGER,
                MINUTES_OF_SLEET_TOTAL INTEGER,
                MINUTES_OF_SNOW_TOTAL INTEGER,
                MINUTES_OF_SUN_TOTAL INTEGER,
                MOISTURE_SOIL_AVG FLOAT,
                MOISTURE_SOIL_MAX FLOAT,
                MOISTURE_SOIL_MIN FLOAT,
                HAS_PRECIPITATION BOOLEAN,
                PRECIPITATION_INTENSITY_MAX FLOAT,
                PRECIPITATION_LWE_TOTAL FLOAT,
                PRECIPITATION_LWE_RATE_AVG FLOAT,
                PRECIPITATION_LWE_RATE_MAX FLOAT,
                PRECIPITATION_LWE_RATE_MIN FLOAT,
                PRECIPITATION_TYPE_PREDOMINANT VARCHAR,
                PRECIPITATION_TYPE_DESC_PREDOMINANT VARCHAR,
                PRESSURE_24HR_DEP FLOAT,
                PRESSURE_AVG FLOAT,
                PRESSURE_MAX FLOAT,
                PRESSURE_MIN FLOAT,
                PRESSURE_MSL_24HR_DEP FLOAT,
                PRESSURE_MSL_AVG FLOAT,
                PRESSURE_MSL_MAX FLOAT,
                PRESSURE_MSL_MIN FLOAT,
                HAS_RAIN BOOLEAN,
                RAIN_LWE_TOTAL FLOAT,
                RAIN_LWE_RATE_AVG FLOAT,
                RAIN_LWE_RATE_MAX FLOAT,
                RAIN_LWE_RATE_MIN FLOAT,
                HAS_SLEET BOOLEAN,
                SLEET_LWE_TOTAL FLOAT,
                SLEET_LWE_RATE_AVG FLOAT,
                SLEET_LWE_RATE_MAX FLOAT,
                SLEET_LWE_RATE_MIN FLOAT,
                HAS_SNOW BOOLEAN,
                SNOW_TOTAL FLOAT,
                SNOW_AVG FLOAT,
                SNOW_MAX FLOAT,
                SNOW_MIN FLOAT,
                SNOW_COVER_24HR_DEP FLOAT,
                SNOW_COVER_AVG FLOAT,
                SNOW_COVER_MAX FLOAT,
                SNOW_COVER_MIN FLOAT,
                SNOW_DEPTH_AVG FLOAT,
                SNOW_DEPTH_MAX FLOAT,
                SNOW_DEPTH_MIN FLOAT,
                SNOW_DRIFTING_INTENSITY_MAX FLOAT,
                SNOW_LIQUID_RATIO_ACCUWEATHER_AVG FLOAT,
                SNOW_LIQUID_RATIO_ACCUWEATHER_MIN FLOAT,
                SNOW_LIQUID_RATIO_ACCUWEATHER_MAX FLOAT,
                SNOW_LIQUID_RATIO_COBB_2005_AVG FLOAT,
                SNOW_LIQUID_RATIO_COBB_2005_MAX FLOAT,
                SNOW_LIQUID_RATIO_COBB_2005_MIN FLOAT,
                SNOW_LIQUID_RATIO_COBB_2011_AVG FLOAT,
                SNOW_LIQUID_RATIO_COBB_2011_MAX FLOAT,
                SNOW_LIQUID_RATIO_COBB_2011_MIN FLOAT,
                SNOW_LIQUID_RATIO_KUCHERA_AVG FLOAT,
                SNOW_LIQUID_RATIO_KUCHERA_MAX FLOAT,
                SNOW_LIQUID_RATIO_KUCHERA_MIN FLOAT,
                SNOW_LIQUID_RATIO_NCEP_AVG FLOAT,
                SNOW_LIQUID_RATIO_NCEP_MAX FLOAT,
                SNOW_LIQUID_RATIO_NCEP_MIN FLOAT,
                SNOW_LWE_TOTAL FLOAT,
                SNOW_LWE_RATE_AVG FLOAT,
                SNOW_LWE_RATE_MAX FLOAT,
                SNOW_LWE_RATE_MIN FLOAT,
                SNOW_TYPE_DESC_PREDOMINANT VARCHAR,
                SOLAR_IRRADIANCE_AVG FLOAT,
                SOLAR_IRRADIANCE_MAX FLOAT,
                SOLAR_IRRADIANCE_TOTAL FLOAT,
                SOLAR_RADIATION_NET_AVG FLOAT,
                SOLAR_RADIATION_NET_MAX FLOAT,
                SOLAR_RADIATION_NET_TOTAL FLOAT,
                TEMPERATURE_24HR_DEP FLOAT,
                TEMPERATURE_AVG FLOAT,
                TEMPERATURE_MAX FLOAT,
                TEMPERATURE_MIN FLOAT,
                TEMPERATURE_DEW_POINT_24HR_DEP FLOAT,
                TEMPERATURE_DEW_POINT_AVG FLOAT,
                TEMPERATURE_DEW_POINT_MAX FLOAT,
                TEMPERATURE_DEW_POINT_MIN FLOAT,
                TEMPERATURE_HEAT_INDEX_24HR_DEP FLOAT,
                TEMPERATURE_HEAT_INDEX_AVG FLOAT,
                TEMPERATURE_HEAT_INDEX_MAX FLOAT,
                TEMPERATURE_HEAT_INDEX_MIN FLOAT,
                TEMPERATURE_REALFEEL_24HR_DEP FLOAT,
                TEMPERATURE_REALFEEL_AVG FLOAT,
                TEMPERATURE_REALFEEL_MAX FLOAT,
                TEMPERATURE_REALFEEL_MIN FLOAT,
                TEMPERATURE_REALFEEL_SHADE_24HR_DEP FLOAT,
                TEMPERATURE_REALFEEL_SHADE_AVG FLOAT,
                TEMPERATURE_REALFEEL_SHADE_MAX FLOAT,
                TEMPERATURE_REALFEEL_SHADE_MIN FLOAT,
                TEMPERATURE_SOIL_24HR_DEP FLOAT,
                TEMPERATURE_SOIL_AVG FLOAT,
                TEMPERATURE_SOIL_MAX FLOAT,
                TEMPERATURE_SOIL_MIN FLOAT,
                TEMPERATURE_WETBULB_24HR_DEP FLOAT,
                TEMPERATURE_WETBULB_AVG FLOAT,
                TEMPERATURE_WETBULB_MAX FLOAT,
                TEMPERATURE_WETBULB_MIN FLOAT,
                TEMPERATURE_WETBULB_GLOBE_24HR_DEP FLOAT,
                TEMPERATURE_WETBULB_GLOBE_AVG FLOAT,
                TEMPERATURE_WETBULB_GLOBE_MAX FLOAT,
                TEMPERATURE_WETBULB_GLOBE_MIN FLOAT,
                TEMPERATURE_WIND_CHILL_24HR_DEP FLOAT,
                TEMPERATURE_WIND_CHILL_AVG FLOAT,
                TEMPERATURE_WIND_CHILL_MAX FLOAT,
                TEMPERATURE_WIND_CHILL_MIN FLOAT,
                VISIBILITY_24HR_DEP FLOAT,
                VISIBILITY_AVG FLOAT,
                VISIBILITY_MAX FLOAT,
                VISIBILITY_MIN FLOAT,
                WIND_DIRECTION_24HR_DEP FLOAT,
                WIND_DIRECTION_AVG FLOAT,
                WIND_DIRECTION_PREDOMINANT VARCHAR,
                WIND_GUST_24HR_DEP FLOAT,
                WIND_GUST_AVG FLOAT,
                WIND_GUST_MAX FLOAT,
                WIND_GUST_MIN FLOAT,
                WIND_GUST_INSTANTANEOUS_24HR_DEP FLOAT,
                WIND_GUST_INSTANTANEOUS_AVG FLOAT,
                WIND_GUST_INSTANTANEOUS_MAX FLOAT,
                WIND_GUST_INSTANTANEOUS_MIN FLOAT,
                WIND_SPEED_24HR_DEP FLOAT,
                WIND_SPEED_AVG FLOAT,
                WIND_SPEED_MAX FLOAT,
                WIND_SPEED_MIN FLOAT
            )
        `;
        await executeQuery(connection, createTableQuery);
        console.log('✅ Created WEATHER_DATA table with all 169 columns');

        // Load the data
        const copyQuery = `
            COPY INTO WEATHER_DATA
            FROM @weather_data_stage/${fileName}
            FILE_FORMAT = (FORMAT_NAME = 'weather_data_format')
            ON_ERROR = 'CONTINUE'
        `;
        const copyResult = await executeQuery(connection, copyQuery);
        console.log('✅ Data loaded successfully:', copyResult.rows);

        // Verify the data
        const countQuery = `SELECT COUNT(*) AS row_count FROM WEATHER_DATA`;
        const countResult = await executeQuery(connection, countQuery);
        console.log(`✅ Loaded ${countResult.rows[0].ROW_COUNT} rows`);

        // Check first row
        const sampleQuery = `SELECT * FROM WEATHER_DATA LIMIT 1`;
        const sampleResult = await executeQuery(connection, sampleQuery);
        console.log('✅ First row sample:', sampleResult.rows[0]);

    } catch (err) {
        console.error('❌ Error during CSV ingestion:', err);
    } finally {
        if (connection) {
            connection.destroy();
        }
    }
}
// Run the ingestion process
ingestCSV();
