import duckdb
from load_staging_finwire_db import staging_finwire_split
from customermgmt_conversion import customermgmt_convert
from staging_data_commands import load_staging
from staging_finwire_load1 import load_finwire
from load_staging_customermgmt_db import load_customermgmt
import time
import pandas as pd


def btrim_equivalent(input_string, trim_characters):
    start_index = len(input_string) - len(input_string.lstrip(trim_characters))
    end_index = len(input_string.rstrip(trim_characters)) - 1
    return input_string[start_index:end_index + 1]


def run_tasks(scale_factor):
    file_path=f'generated_data/sf_{scale_factor}'

    with open('create_staging_schema.sql', 'r') as sql_file:
        create_staging_schema = sql_file.read()

    with open('create_master_schema.sql', 'r') as sql_file:
        create_master_schema = sql_file.read()

    with open('transformations/1_load_master_tradetype.sql', 'r') as sql_file:
        load_master_tradetype = sql_file.read()

    with open('transformations/2_load_master_statustype.sql', 'r') as sql_file:
        load_master_statustype = sql_file.read()

    with open('transformations/3_load_master_taxrate.sql', 'r') as sql_file:
        load_master_taxrate = sql_file.read()

    with open('transformations/4_load_master_industry.sql', 'r') as sql_file:
        load_master_industry = sql_file.read()

    with open('transformations/5_transform_load_master_dimdate.sql', 'r') as sql_file:
        transform_load_master_dimdate = sql_file.read()

    with open('transformations/6_transform_load_master_dimtime.sql', 'r') as sql_file:
        transform_load_master_dimtime = sql_file.read()

    with open('transformations/7_transform_load_master_dimcompany.sql', 'r') as sql_file:
        transform_load_master_dimcompany = sql_file.read()

    with open('transformations/8_load_master_dimessages_dimcompany.sql', 'r') as sql_file:
        load_master_dimessages_dimcompany = sql_file.read()

    with open('transformations/9_transform_load_master_dimbroker.sql', 'r') as sql_file:
        transform_load_master_dimbroker = sql_file.read()

    with open('transformations/10_transform_load_master_prospect.sql', 'r') as sql_file:
        transform_load_master_prospect = sql_file.read()

    with open('transformations/11_transform_load_master_dimcustomer.sql', 'r') as sql_file:
        transform_load_master_dimcustomer = sql_file.read()

    with open('transformations/12_load_master_dimessages_dimcustomer.sql', 'r') as sql_file:
        load_master_dimessages_dimcustomer = sql_file.read()

    with open('transformations/13_update_master_prospect.sql', 'r') as sql_file:
        update_master_prospect = sql_file.read()

    with open('transformations/14_transform_load_master_dimaccount.sql', 'r') as sql_file:
        transform_load_master_dimaccount = sql_file.read()

    with open('transformations/15_transform_load_master_dimsecurity.sql', 'r') as sql_file:
        transform_load_master_dimsecurity = sql_file.read()

    with open('transformations/16_2_transform_load_master_dimtrade.sql', 'r') as sql_file:
        transform_load_master_dimtrade = sql_file.read()

    with open('transformations/17_load_master_dimessages_dimtrade.sql', 'r') as sql_file:
        load_master_dimessages_dimtrade = sql_file.read()

    with open('transformations/18_transform_load_master_financial.sql', 'r') as sql_file:
        transform_load_master_financial = sql_file.read()

    with open('transformations/19_transform_load_master_factcashbalances.sql', 'r') as sql_file:
        transform_load_master_factcashbalances = sql_file.read()

    with open('transformations/20_transform_load_master_factholdings.sql', 'r') as sql_file:
        transform_load_master_factholdings = sql_file.read()

    with open('transformations/21_transform_load_master_factwatches.sql', 'r') as sql_file:
        transform_load_master_factwatches = sql_file.read()

    with open('transformations/22_transform_load_master_factmarkethistory.sql', 'r') as sql_file:
        transform_load_master_factmarkethistory = sql_file.read()

    with open('transformations/23_load_master_dimessages_factmarkethistory.sql', 'r') as sql_file:
        load_master_dimessages_factmarkethistory = sql_file.read()

    with open('transformations/24_load_s_trade_joined.sql', 'r') as sql_file:
        load_s_trade_joined = sql_file.read()

    con = duckdb.connect(f'created_db/sf_{scale_factor}.db')

    con.create_function("btrim_equivalent", btrim_equivalent, ['VARCHAR', 'VARCHAR'], 'VARCHAR')

    res = {}

    # Task 1 - Create staging schema
    print('Task 1 - Create staging schema...')
    create_staging_schema_start = time.time()
    con.sql(create_staging_schema)
    create_staging_schema_end = time.time()
    res["create_staging_schema"] = [create_staging_schema_end - create_staging_schema_start]

    # Task 2 - Load txt and csv sources to staging
    print('Task 2 - Load txt and csv sources to staging...')
    load_staging(con,file_path)
    load_txt_csv_sources_to_staging_end = time.time()
    res["load_txt_csv_sources_to_staging"] = [load_txt_csv_sources_to_staging_end - create_staging_schema_end]

    # Task 3 - Load finwire source to staging
    print('Task 3 - Load finwire source to staging...')
    load_finwire(con,file_path)
    load_finwire_to_staging_end = time.time()
    res["load_finwire_to_staging"] = [load_finwire_to_staging_end - load_txt_csv_sources_to_staging_end]

    # Task 4 - Parse finwire and load to seperate tables
    print('Task 4 - Parse finwire and load to seperate tables...')
    staging_finwire_split(conn=con)
    parse_finwire_end = time.time()
    res["parse_finwire"] = [parse_finwire_end - load_finwire_to_staging_end]

    # Task 5 - Convert customer management source from xml to csv
    print('Task 5 - Convert customer management source from xml to csv...')
    xml_path = file_path + "/Batch1"
    customermgmt_convert(xml_path)
    convert_customermgmt_xml_to_csv_end = time.time()
    res["convert_customermgmt_xml_to_csv"] = [convert_customermgmt_xml_to_csv_end - parse_finwire_end]

    # Task 6 - Load customer management source to staging
    print('Task 6 - Load customer management source to staging')
    load_customermgmt(con,file_path)
    load_customer_mgmt_to_staging_end = time.time()
    res["load_customer_mgmt_to_staging"] = [load_customer_mgmt_to_staging_end - convert_customermgmt_xml_to_csv_end]

    # Task 7 - Create master schema
    print('Task 7 - Create master schema')
    con.sql(create_master_schema)
    create_master_schema_end = time.time()
    res["create_master_schema"] = [create_master_schema_end - load_customer_mgmt_to_staging_end]

    # Task 8, 9, 10, 11 - Direct load master.tradetype, master.statustype, master.taxrate, master.industry
    print('Task 8, 9, 10, 11 - Direct load master.tradetype, master.statustype, master.taxrate, master.industry...')
    con.sql(load_master_tradetype)
    load_master_tradetype_end = time.time()
    res["load_master_tradetype"] = [load_master_tradetype_end - create_master_schema_end]

    con.sql(load_master_statustype)
    load_master_statustype_end = time.time()
    res["load_master_statustype"] = [load_master_statustype_end - load_master_tradetype_end]

    con.sql(load_master_taxrate)
    load_master_taxrate_end = time.time()
    res["load_master_taxrate"] = [load_master_taxrate_end - load_master_statustype_end]

    con.sql(load_master_industry)
    load_master_industry_end = time.time()
    res["load_master_industry"] = [load_master_industry_end - load_master_taxrate_end]

    # Task 12, 13, 14 - Transform & load master.dimdate, master.dimtime, master.dimcompany
    print('Task 12, 13, 14 - Transform & load master.dimdate, master.dimtime, master.dimcompany...')
    con.sql(transform_load_master_dimdate)
    transform_load_master_dimdate_end = time.time()
    res["transform_load_master_dimdate"] = [transform_load_master_dimdate_end - load_master_industry_end]

    con.sql(transform_load_master_dimtime)
    transform_load_master_dimtime_end = time.time()
    res['transform_load_master_dimtime'] = [transform_load_master_dimtime_end - transform_load_master_dimdate_end]

    con.sql(transform_load_master_dimcompany)
    transform_load_master_dimcompany_end = time.time()
    res["transform_load_master_dimcompany"] = [transform_load_master_dimcompany_end - transform_load_master_dimtime_end]

    # Task 15 - Load master.dimessages with alert from master.dimcompany
    print('Task 15 - Load master.dimessages with alert from master.dimcompany...')
    con.sql(load_master_dimessages_dimcompany)
    load_master_dimessages_dimcompany_end = time.time()
    res["load_master_dimessages_dimcompany"] = [load_master_dimessages_dimcompany_end - transform_load_master_dimcompany_end]

    # Task 16, 17, 18, 19 - Transform & load master.dimbroker, prospect, customer, dimessages_customer
    print('Task 16, 17, 18, 19 - Transform & load master.dimbroker, prospect, customer, dimessages_customer...')
    con.sql(transform_load_master_dimbroker)
    transform_load_master_dimbroker_end = time.time()
    res["transform_load_master_dimbroker"] = [transform_load_master_dimbroker_end - load_master_dimessages_dimcompany_end]

    con.sql(transform_load_master_prospect)
    transform_load_master_prospect_end = time.time()
    res["transform_load_master_prospect"] = [transform_load_master_prospect_end - transform_load_master_dimbroker_end]

    con.sql(transform_load_master_dimcustomer)
    transform_load_master_dimcustomer_end = time.time()
    res["transform_load_master_dimcustomer"] = [transform_load_master_dimcustomer_end - transform_load_master_prospect_end]

    con.sql(load_master_dimessages_dimcustomer)
    load_master_dimessages_dimcustomer_end = time.time()
    res["load_master_dimessages_dimcustomer"] = [load_master_dimessages_dimcustomer_end - transform_load_master_dimcustomer_end]

    # Task 20 - Update master.prospect
    print('Task 20 - Update master.prospect...')
    con.sql(update_master_prospect)
    update_master_prospect_end = time.time()
    res["update_master_prospect"] = [update_master_prospect_end - load_master_dimessages_dimcustomer_end]

    # Task 21 - Transform & load master.dimaccount
    print('Task 21 - Transform & load master.dimaccount...')
    con.sql(transform_load_master_dimaccount)
    transform_load_master_dimaccount_end = time.time()
    res["transform_load_master_dimaccount"] = [transform_load_master_dimaccount_end - update_master_prospect_end]

    # Task 22 - Transform & load master.dimsecurity
    print('Task 22 - Transform & load master.dimsecurity...')
    con.sql(transform_load_master_dimsecurity)
    transform_load_master_dimsecurity_end = time.time()
    res["transform_load_master_dimsecurity"] = [transform_load_master_dimsecurity_end - transform_load_master_dimaccount_end]

    # Task 23 - Transform & load master.dimtrade, divided into 2 steps
    print('Task 23 - Transform & load master.dimtrade, divided into 2 steps...')
    con.sql(load_s_trade_joined)
    con.sql(transform_load_master_dimtrade)
    transform_load_master_dimtrade_end = time.time()
    res["transform_load_master_dimtrade"] = [transform_load_master_dimtrade_end - transform_load_master_dimsecurity_end]

    # Task 24 - Load master.dimessages with alert from master.dimtrade
    print('Task 24 - Load master.dimessages with alert from master.dimtrade...')
    con.sql(load_master_dimessages_dimtrade)
    load_master_dimessages_dimtrade_end = time.time()
    res["load_master_dimessages_dimtrade"] = [load_master_dimessages_dimtrade_end - transform_load_master_dimtrade_end]

    # Task 25 - Transform & load master.financial
    print('Task 25 - Transform & load master.financial...')
    con.sql(transform_load_master_financial)
    transform_load_master_financial_end = time.time()
    res["transform_load_master_financial"] = [transform_load_master_financial_end - load_master_dimessages_dimtrade_end]

    # Task 26 - Transform & load master.factcashbalances
    print('Task 26 - Transform & load master.factcashbalances...')
    con.sql(transform_load_master_factcashbalances)
    transform_load_master_factcashbalances_end = time.time()
    res["transform_load_master_factcashbalances"] = [transform_load_master_factcashbalances_end - transform_load_master_financial_end]

    # Task 27 - Transform & load master.factholdings
    print('Task 27 - Transform & load master.factholdings...')
    con.sql(transform_load_master_factholdings)
    transform_load_master_factholdings_end = time.time()
    res["transform_load_master_factholdings"] = [transform_load_master_factholdings_end - transform_load_master_factcashbalances_end]

    # Task 28 - Transform & load master.factwatches
    print('Task 28 - Transform & load master.factwatches...')
    con.sql(transform_load_master_factwatches)
    transform_load_master_factwatches_end = time.time()
    res["transform_load_master_factwatches"] = [transform_load_master_factwatches_end - transform_load_master_factholdings_end]

    # Task 29 - Transform & load master.factmarkethistory
    print('Task 29 - Transform & load master.factmarkethistory...')
    con.sql(transform_load_master_factmarkethistory)
    transform_load_master_factmarkethistory_end = time.time()
    res["transform_load_master_factmarkethistory"] = [transform_load_master_factmarkethistory_end - transform_load_master_factwatches_end]

    # Task 30 - Load master.dimessages with alert from master.factmarkethistory
    print('Task 30 - Load master.dimessages with alert from master.factmarkethistory...')
    con.sql(load_master_dimessages_factmarkethistory)
    load_master_dimessages_factmarkethistory_end = time.time()
    res["load_master_dimessages_factmarkethistory"] = [load_master_dimessages_factmarkethistory_end - transform_load_master_factmarkethistory_end]

    df = pd.DataFrame(res)
    df.to_csv(f'results/result_sf{scale_factor}.csv', index=False)
    print(f'Benchmark run sucessfully. The result is saved at results/result_sf{scale_factor}.csv')


if __name__ == "__main__":
    sf = 3
    run_tasks(scale_factor=sf)