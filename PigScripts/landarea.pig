file_input = LOAD 'lab6/gaz_tracts_national.txt' USING PigStorage('\t') AS (state:chararray, geoid:long, pop10:long, hu10:long, aland:long, awater:long, alandsqmi:float, awatersqmi:float, lat:float, lang:float);
-- filter_input = FILTER file_input BY state == 'AK';
records = FOREACH file_input GENERATE state, aland;
statebag = GROUP records BY state;
totals = FOREACH statebag {
		total = SUM(records.aland);
		st = DISTINCT records.state;
		GENERATE FLATTEN(st) AS state, total as land_area;
	}
top10 = LIMIT (ORDER totals BY land_area DESC) 10;
dump top10;
STORE top10 INTO 'lab6/test';
