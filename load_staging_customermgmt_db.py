import duckdb

def load_customermgmt(con, folder_path):
    original_copy_command = '''
    DELETE FROM staging.customermgmt;

    -- Assuming you have headers in your CSV file. If not, use HEADER FALSE.
    COPY staging.customermgmt FROM 'generated_data/Batch1/CustomerMgmt.csv' WITH CSV HEADER;

    '''
    new_copy_command = original_copy_command.replace("generated_data", folder_path)
    con.sql(new_copy_command)