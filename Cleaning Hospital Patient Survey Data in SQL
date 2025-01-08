-- This project is based on a youtube project from Data Wizardry with a few key differences
	-- First, the original project used PostgreSQL so I had to figure out how that translates to MySQL
	-- Second, the original project used data from across the US but I wanted to filter down to North Carolina to make it more relevant

-- 1. Create the tables and then import the CSV data into them

CREATE TABLE hospitals.hospital_beds
	(
	provider_ccn INT,
	hospital_name VARCHAR(255), 
	fiscal_year_begin_date VARCHAR(10), 
	fiscal_year_end_date VARCHAR(10),
	number_of_beds INT
	);

CREATE TABLE hospitals.HCAHPS_data
	(
    facility_id VARCHAR(10),
    facility_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(50),
    telephone_number VARCHAR(20),
    hcahps_measure_id VARCHAR(255),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    hcahps_answer_percent INT,
    num_completed_surveys INT,
    survey_response_rate_percent INT,
    start_date VARCHAR(10),
    end_date VARCHAR(10)
	);
    
-- 2. Review data in new tables
	-- Note: I use LIMIT while I'm formatting my tables to save database resources as the tables have thousands of rows

SELECT
	*
FROM
	hospitals.hcahps_data
LIMIT
	10
;

SELECT
	*
FROM
	hospitals.hospital_beds
LIMIT
	10
;

-- 3. In hostpital_beds table, change format of dates from string-based to date-based so that I can sort by date
	-- First I need to figure out how to do this in MySQL since the youtube project is using PostgreSQL
		-- https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_str-to-date
SELECT
	fiscal_year_begin_date,
    STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS reformatted_fiscal_year_begin_date, 
    fiscal_year_end_date,
    STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS reformatted_fiscal_year_end_date
FROM
	hospitals.hospital_beds
LIMIT
	5
;

		-- Ok, it works! Now I make the changes in my broader code for both fiscal_year_begin_date and fiscal_year_end_date columns
    
SELECT
	provider_ccn,
    hospital_name,
	STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
	STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
    number_of_beds
FROM
	hospitals.hospital_beds
LIMIT
	5
;

-- 4. In hospital_beds table, ensure provider_ccn is 6 digits (some leading zeros have been removed)
	-- First I need to figure out how to do this in MySQL since the youtube project is using PostgreSQL
		-- https://stackoverflow.com/questions/11165104/adding-a-leading-zero-to-some-values-in-column-in-mysql
        -- https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_lpad
SELECT
	provider_ccn,
    LPAD(provider_ccn,6,'0') AS reformatted_provider_ccn,
    hospital_name
FROM
	hospitals.hospital_beds
LIMIT
	10
;

		-- Ok, it works! Now I make the changes in my broader code 

SELECT
	LPAD(provider_ccn,6,'0') AS provider_ccn,
    hospital_name,
	STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
	STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
    number_of_beds
FROM
	hospitals.hospital_beds
LIMIT
	10
;

-- 5. In hostpital_beds table, some hospitals have multiple rows with different dates (due to them sending in data multiple times) which gives me multiple choices for number_of_beds
	-- I need to sort the dates and remove the older instances so I only have the newest instance for each hospital with the most updated number_of_beds information
   
	-- First create a CTE https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions
    
WITH hospital_beds_prep AS
	(
	SELECT
		LPAD(provider_ccn,6,'0') AS provider_ccn,
		hospital_name,
		STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
		STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
		number_of_beds
	FROM
		hospitals.hospital_beds
	)
    
    -- Now test that the CTE 'hospital_beds_prep' is the same as the original table 'hospital_beds'
    
    SELECT
		*
	FROM 
		hospital_beds_prep
    ;
    
	-- Success!
    
	-- Now add a partitioning statement to the new CTE to add a "1" to the most recent row IF hospitals have multiple rows of data, then "2" for second most recent, etc
		-- https://dev.mysql.com/doc/refman/8.4/en/window-function-descriptions.html#function_row-number
        -- https://dev.mysql.com/doc/refman/8.4/en/window-functions-usage.html
        -- This part caused me quite a bit of trouble so I had help debugging with GenAI (ChatGPT)
    
    WITH hospital_beds_prep AS
	(
	SELECT
		LPAD(provider_ccn,6,'0') AS provider_ccn,
		hospital_name,
		STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
		STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
		number_of_beds,
        ROW_NUMBER() OVER (
			PARTITION BY LPAD(provider_ccn,6,'0') 
            ORDER BY STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') DESC
	) AS nth_row
	FROM
		hospitals.hospital_beds
	)
    SELECT 
		*
	FROM
		hospital_beds_prep
	ORDER BY provider_ccn ASC
	;
    
    -- Hmm I'm missing provider_ccn 053308 which was called out as an example in the youtube of having multiple rows. I need to experiment to see where the rows were dropped!
    
 	SELECT
		provider_ccn,
        hospital_name
	FROM 
		hospitals.hospital_beds
	WHERE 
		provider_ccn = 053308
    ;
    
	-- Interesting! It's not showing up in the original table either in MySQL; however it is showing up in the original CSV file so something must have happened while importing the data... 
		-- Ok cross-referncing 53308 with the HCAHPS_data table, it is also not showing up in that table so it will not be included when we JOIN the tables
		-- Therefore, I have decided to just keep going instead of importing the table again and starting over
		-- BUT I should always check the number of rows at the very beginning to ensure they match the original file!
		-- Just in case, I also confirmed that other examples he highlighted as having multiple rows are included in the dataset (051321 and 054157)
    
	-- Ok we have 1, 2, 3, etc rows sorted and now need to remove any that are not "1"
    
	WITH hospital_beds_prep AS
	(
	SELECT
		LPAD(provider_ccn,6,'0') AS provider_ccn,
		hospital_name,
		STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
		STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
		number_of_beds,
        ROW_NUMBER() OVER (
			PARTITION BY LPAD(provider_ccn,6,'0') 
            ORDER BY STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') DESC
	) AS nth_row
	FROM
		hospitals.hospital_beds
	)
    SELECT 
		provider_ccn,
        COUNT(*) AS count_rows
	FROM
		hospital_beds_prep
	WHERE
		nth_row = 1
	GROUP BY
		provider_ccn
	ORDER BY
		COUNT(*) DESC
	;
    
-- 6. Great! Now to format the HCAHPS_data table 
	-- First, add in the leading zeros to facility_id (which is the same as the provider_cnn in the first table)

SELECT
	LPAD(facility_id,6,'0') AS provider_ccn,
	facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county_or_parish,
    telephone_number,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_answer_percent,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date
FROM
	hospitals.hcahps_data
;
    
	-- Next, convert start_date and end_date back to date based formats from text based

SELECT
	LPAD(facility_id,6,'0') AS provider_ccn,
	facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county_or_parish,
    telephone_number,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_answer_percent,
    num_completed_surveys,
    survey_response_rate_percent,
	STR_TO_DATE(start_date, '%m/%d/%Y') AS start_date, 
	STR_TO_DATE(end_date, '%m/%d/%Y') AS end_date
FROM
	hospitals.hcahps_data
;

-- 7. Finally JOIN the tables together to include number_of_beds column from hospital_beds into the bigger HCAHPS table

	WITH hospital_beds_prep AS
	(
	SELECT
		LPAD(provider_ccn,6,'0') AS provider_ccn,
		hospital_name,
		STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
		STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
		number_of_beds,
        ROW_NUMBER() OVER (
			PARTITION BY LPAD(provider_ccn,6,'0') 
            ORDER BY STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') DESC
	) AS nth_row
	FROM
		hospitals.hospital_beds
	)

SELECT
	LPAD(facility_id,6,'0') AS provider_ccn,
	facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county_or_parish,
    telephone_number,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_answer_percent,
    num_completed_surveys,
    survey_response_rate_percent,
	STR_TO_DATE(start_date, '%m/%d/%Y') AS start_date, 
	STR_TO_DATE(end_date, '%m/%d/%Y') AS end_date,
    beds.number_of_beds,
    beds.fiscal_year_begin_date AS beds_start_report_period,
    beds.fiscal_year_end_date AS beds_end_report_period
FROM
	hospitals.hcahps_data AS HCAHPS
LEFT JOIN hospital_beds_prep AS Beds
	ON LPAD(provider_ccn,6,'0') = Beds.provider_ccn
AND beds.nth_row = 1
;

-- 8. And finally, filter only hospitals in NC

	WITH hospital_beds_prep AS
	(
	SELECT
		LPAD(provider_ccn,6,'0') AS provider_ccn,
		hospital_name,
		STR_TO_DATE(fiscal_year_begin_date, '%m/%d/%Y') AS fiscal_year_begin_date, 
		STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') AS fiscal_year_end_date,
		number_of_beds,
        ROW_NUMBER() OVER (
			PARTITION BY LPAD(provider_ccn,6,'0') 
            ORDER BY STR_TO_DATE(fiscal_year_end_date, '%m/%d/%Y') DESC
	) AS nth_row
	FROM
		hospitals.hospital_beds
	)

SELECT
	LPAD(facility_id,6,'0') AS provider_ccn,
	facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county_or_parish,
    telephone_number,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_answer_percent,
    num_completed_surveys,
    survey_response_rate_percent,
	STR_TO_DATE(start_date, '%m/%d/%Y') AS start_date, 
	STR_TO_DATE(end_date, '%m/%d/%Y') AS end_date,
    beds.number_of_beds,
    beds.fiscal_year_begin_date AS beds_start_report_period,
    beds.fiscal_year_end_date AS beds_end_report_period
FROM
	hospitals.hcahps_data AS HCAHPS
LEFT JOIN hospital_beds_prep AS Beds
	ON LPAD(provider_ccn,6,'0') = Beds.provider_ccn
AND beds.nth_row = 1
AND HCAHPS.state = 'NC'
;

-- And I'm done! 
