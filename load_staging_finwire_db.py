import duckdb

def staging_finwire_split(conn):
    # Function to replace PL/pgSQL logic
    
    # Truncate tables
    conn.execute("DELETE FROM staging.finwire_cmp")
    conn.execute("DELETE FROM staging.finwire_sec")
    conn.execute("DELETE FROM staging.finwire_fin")
    
    # Fetch data from staging.finwire
    result = conn.execute("SELECT * FROM staging.finwire").fetchall()
    for row in result:
        rectype = row[0][15:18]
        if rectype == 'CMP':
            # Logic for CMP
            exe_args = [row[0] for i in range(16)]
            conn.execute("""
                INSERT INTO staging.finwire_cmp 
                SELECT 
					nullif(trim(both from substring(?,1,15)), '') as pts,
					nullif(trim(both from substring(?,16,3)), '') as rectype,
					nullif(trim(both from substring(?,19,60)), '') as companyname,
					nullif(trim(both from substring(?,79,10)), '') as cik,
					nullif(trim(both from substring(?,89,4)), '') as status,
					nullif(trim(both from substring(?,93,2)), '') as industryid,
					nullif(trim(both from substring(?,95,4)), '') as sprating,
					nullif(trim(both from substring(?,99,8)), '') as foundingdate,
					nullif(trim(both from substring(?,107,80)), '') as addressline1,
					nullif(trim(both from substring(?,187,80)), '') as addressline2,
					nullif(trim(both from substring(?,267,12)), '') as postalcode,
					nullif(trim(both from substring(?,279,25)), '') as city,
					nullif(trim(both from substring(?,304,20)), '') as stateprovince,
					nullif(trim(both from substring(?,324,24)), '') as country,
					nullif(trim(both from substring(?,348,46)), '') as ceoname,
					nullif(trim(both from substring(?,394,150)), '') as description
                LIMIT 1
            """, exe_args)
        elif rectype == 'SEC':
            # Logic for SEC
            exe_args = [row[0] for i in range(12)]
            conn.execute("""
                INSERT INTO staging.finwire_sec 
                SELECT 
					nullif(trim(both from substring(?,1,15)), '') as pts,
					nullif(trim(both from substring(?,16,3)), '') as rectype,
					nullif(trim(both from substring(?,19,15)), '') as symbol,
					nullif(trim(both from substring(?,34,6)), '') as issuetype,
					nullif(trim(both from substring(?,40,4)), '') as status,
					nullif(trim(both from substring(?,44,70)), '') as name,
					nullif(trim(both from substring(?,114,6)), '') as exid,
					nullif(trim(both from substring(?,120,13)), '') as shout,
					nullif(trim(both from substring(?,133,8)), '') as firsttradedate,
					nullif(trim(both from substring(?,141,8)), '') as firsttradeexchg,
					nullif(trim(both from substring(?,149,12)), '') as dividend,
					nullif(trim(both from substring(?,161,60)), '') as conameorcik	
                LIMIT 1
            """, exe_args)
        elif rectype == 'FIN':
            # Logic for FIN
            exe_args = [row[0] for i in range(17)]
            conn.execute("""
                INSERT INTO staging.finwire_fin 
                SELECT 
					nullif(trim(both from substring(?,1,15)), '') as pts,
					nullif(trim(both from substring(?,16,3)), '') as rectype,
					nullif(trim(both from substring(?,19,4)), '') as year,
					nullif(trim(both from substring(?,23,1)), '') as quarter,
					nullif(trim(both from substring(?,24,8)), '') as qtrstartdate,
					nullif(trim(both from substring(?,32,8)), '') as postingdate,
					nullif(trim(both from substring(?,40,17)), '') as revenue,
					nullif(trim(both from substring(?,57,17)), '') as earnings,
					nullif(trim(both from substring(?,74,12)), '') as eps,
					nullif(trim(both from substring(?,86,12)), '') as dilutedeps,
					nullif(trim(both from substring(?,98,12)), '') as margin,
					nullif(trim(both from substring(?,110,17)), '') as inventory,
					nullif(trim(both from substring(?,127,17)), '') as assets,
					nullif(trim(both from substring(?,144,17)), '') as liability,
					nullif(trim(both from substring(?,161,13)), '') as shout,
					nullif(trim(both from substring(?,174,13)), '') as dilutedshout,
					nullif(trim(both from substring(?,187,60)), '') as conameorcik
                LIMIT 1
            """, exe_args)

# Connect to DuckDB
# conn = duckdb.connect(database=':memory:')

# Execute the function
# staging_finwire_split(conn)

# Close the connection
# conn.close()
