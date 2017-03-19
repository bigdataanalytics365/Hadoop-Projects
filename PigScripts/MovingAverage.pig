-- Register the jar file
REGISTER '/home/rvshah/lab7/stockanalysis.jar';
-- LOAD data from csv file
file_input = LOAD '/class/s17419/lab7/historicaldata.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:float, high:float, low:float, close:float, volume:long);
-- file_input = LOAD '/home/rvshah/lab7/test.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:chararray, high:chararray, low:chararray, close:chararray, volume:long);
input_data = FOREACH file_input GENERATE ticker AS company, date, open;
-- All records before Oct 1st 2013.
first_date_data = FILTER input_data BY date <= 20131001;
-- All records before Oct 31st 2013.
filtered_data = FILTER input_data BY date <= 20131031;
-- Group the input data by company.
grouped_data = GROUP filtered_data BY company;
-- Compute the starting dates for each company.
start_data = FOREACH (GROUP first_date_data BY company) {
    dates = first_date_data.date;
    ordered_dates = ORDER dates BY date DESC;
    -- Compute the starting date for each company.
    start = stockanalysis.StartDate(ordered_dates);
    GENERATE group AS symbl, start AS first_date;
}
-- Join the starting date data with the stock price data.
joined_data = JOIN filtered_data BY company, start_data BY symbl;
-- Group everything by the company name/symbl.
stock_data = GROUP joined_data BY company;
-- Compute the running averages of companies from grouped data.
company_averages = FOREACH stock_data {
    -- Filter out all records except for the desired range.
    filtered_dates = FILTER joined_data BY date >= first_date;
    -- Order the records in ASC fashion so we compute the correct averages for each date in october.
    ordered_dates = ORDER filtered_dates BY date ASC;
    GENERATE FLATTEN(stockanalysis.MovingAverage(ordered_dates)) AS avgs;
}
-- Dump the result averages for first company.
test = LIMIT company_averages 31;
DUMP test;
-- Store the averages in hdfs.
STORE company_averages INTO '/scr/rvshah/lab7/exp2/moving_averages/';
