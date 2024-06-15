import duckdb

def load_staging(con, folder_path):
    original_copy_command = '''
    COPY staging.batchdate FROM 'generated_data/Batch1/BatchDate.txt';

    COPY staging.cashtransaction FROM 'generated_data/Batch1/CashTransaction.txt' delimiter '|';

    COPY staging.dailymarket FROM 'generated_data/Batch1/DailyMarket.txt' delimiter '|';

    COPY staging.date FROM 'generated_data/Batch1/Date.txt' delimiter '|';

    COPY staging.holdinghistory FROM 'generated_data/Batch1/HoldingHistory.txt' delimiter '|';

    COPY staging.hr FROM 'generated_data/Batch1/HR.csv' delimiter ',' CSV;

    COPY staging.industry FROM 'generated_data/Batch1/Industry.txt' delimiter '|';

    COPY staging.prospect FROM 'generated_data/Batch1/Prospect.csv' delimiter ',' CSV;

    COPY staging.statustype FROM 'generated_data/Batch1/StatusType.txt' delimiter '|';

    COPY staging.taxrate FROM 'generated_data/Batch1/TaxRate.txt' delimiter '|';

    COPY staging.time FROM 'generated_data/Batch1/Time.txt' delimiter '|';

    COPY staging.tradehistory FROM 'generated_data/Batch1/TradeHistory.txt' delimiter '|';

    COPY staging.trade FROM 'generated_data/Batch1/Trade.txt' delimiter '|' null as '';

    COPY staging.tradetype FROM 'generated_data/Batch1/TradeType.txt' delimiter '|';

    COPY staging.watchhistory FROM 'generated_data/Batch1/WatchHistory.txt' delimiter '|';

    COPY staging.audit FROM 'generated_data/Batch1_audit.csv' DELIMITER ',' HEADER CSV NULL AS '';
    '''
    new_copy_command = original_copy_command.replace("generated_data",folder_path)
    con.sql(new_copy_command)
