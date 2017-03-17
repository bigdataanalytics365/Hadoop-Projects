-- Register the jar file
REGISTER 'lab7/stockanalysis.jar';
-- LOAD data from csv file
-- file_input = LOAD '/class/s17419/lab7/historicaldata.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:float, high:float, low:float, close:float, volume:long);
file_input = LOAD '/home/rvshah/lab7/test.csv' USING PigStorage(',') AS (ticker:chararray, date:long, open:chararray, high:chararray, low:chararray, close:chararray, volume:long);
input_data = FOREACH file_input GENERATE ticker, date, open;
-- Period one companies from 19900101 to 20000103 (i.e.) Jan 1st 1990 to Jan 3rd 2000
period_one = FILTER input_data BY date >= 19900101 AND date <= 20000103;
-- Period two companies from 20050102 to 20140131 (i.e.) Jan 2nd 2005 to Jan 31st 2014
period_two = FILTER input_data BY date >= 20050102 AND date <= 20140131;
-- Group the period_one and period_two records by the company and generate tuples for starting price and ending price.
grouped_one = GROUP period_one BY ticker;
grouped_two = GROUP period_two BY ticker;
-- Compute the period one growth rates
open_price_one = FOREACH grouped_one {
    first_ordered = ORDER period_one BY date ASC;
    first_available = LIMIT first_ordered 1;
    last_ordered = ORDER period_one BY date DESC;
    last_available = LIMIT last_ordered 1;
    GENERATE group AS company, FLATTEN(first_available.open) AS first_open, FLATTEN(last_available.open) AS last_open;
}
-- Compute the period two growth rates
open_price_two = FOREACH grouped_two {
    first_ordered = ORDER period_two BY date ASC;
    first_available = LIMIT first_ordered 1;
    last_ordered = ORDER period_two BY date DESC;
    last_available = LIMIT last_ordered 1;
    GENERATE group AS company, FLATTEN(first_available.open) AS first_open, FLATTEN(last_available.open) AS last_open;
}
-- Get companies that only appear for one day in each perid, (i.e) Only has one record in the time period.
growth_factor_one = FOREACH open_price_one {
    growth_factor = stockanalysis.GrowthFactor(temp);
    GENERATE open_price_one.company, temp;
}
DUMP open_price_one;
DUMP growth_factor_one;
-- DUMP open_price_two;
-- test1 = LIMIT grouped_one 1;
-- test2 = LIMIT grouped_two 1;
-- DUMP test1;
-- DUMP test2;