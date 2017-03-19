-- Register the jar file
REGISTER '/home/rvshah/lab7/stockanalysis.jar';
-- LOAD data from csv file
-- file_input = LOAD '/class/s17419/lab7/historicaldata.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:float, high:float, low:float, close:float, volume:long);
file_input = LOAD '/home/rvshah/lab7/test.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:chararray, high:chararray, low:chararray, close:chararray, volume:long);
input_data = FOREACH file_input GENERATE ticker AS company, date, open;
first_date_data = FILTER input_data BY date <= 20131001;
filtered_data = FILTER input_data BY date <= 20131031;
-- Group the input data by company.
grouped_data = GROUP filtered_data BY company;
start_data = FOREACH (GROUP first_date_data BY company) {
    dates = first_date_data.date;
    ordered_dates = ORDER dates BY date DESC;
    start = stockanalysis.StartDate(ordered_dates);
    GENERATE group AS symbl, start AS first_date;
}
joined_data = JOIN filtered_data BY company, start_data BY symbl;
stock_data = GROUP joined_data BY company;
company_averages = FOREACH stock_data {
    filtered_dates = FILTER joined_data BY date >= first_date;
    avgs = FOREACH filtered_dates GENERATE stockanalysis.MovingAverage(filtered_dates);
    GENERATE company, avgs;
}
-- jf_data = FILTER joined_data BY filtered_data.date >= start_data.first_date;
test = LIMIT company_averages 5;
DUMP test;

-- -- Store the growths in respective places.
-- STORE largest_growth_one INTO '/scr/rvshah/lab7/exp1/first_period/';
-- STORE largest_growth_two INTO '/scr/rvshah/lab7/exp1/second_period/';
