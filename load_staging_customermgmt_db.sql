DELETE FROM staging.customermgmt;

-- Assuming you have headers in your CSV file. If not, use HEADER FALSE.
COPY staging.customermgmt FROM 'generated_data/Batch1/CustomerMgmt.csv' WITH CSV HEADER;
