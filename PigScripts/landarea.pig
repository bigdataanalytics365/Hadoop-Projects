-- LOAD data from file
file_input = LOAD 'lab6/gaz_tracts_national.txt' USING PigStorage('\t') AS (state:chararray, geoid:long, pop10:long, hu10:long, aland:long, awater:long, alandsqmi:float, awatersqmi:float, lat:float, lang:float);
-- Generate the necessary columns needed.
records = FOREACH file_input GENERATE state, aland;
-- Group the records relation by state 
statebag = GROUP records BY state;
-- For each of the state generate the state name and the count of the total land area.
totals = FOREACH statebag {
		total = SUM(records.aland);
		st = DISTINCT records.state;
		GENERATE FLATTEN(st) AS state, total as land_area;
	}
-- Order the totals by the land area descending and limit number of records to 10.
top10 = LIMIT (ORDER totals BY land_area DESC) 10;
-- Print out thte results in terminal.
dump top10;
-- Store the output into a file
STORE top10 INTO 'lab6/landarea';
