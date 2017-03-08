file_input = LOAD '/class/s17419/lab6/network_trace' USING PigStorage(' ') AS (time:chararray, word:chararray, src:chararray, sign:chararray, dst:chararray, protocol:chararray, extra:chararray);
tcp_input = FILTER file_input BY protocol == 'tcp';
filter_input = FOREACH tcp_input GENERATE SUBSTRING(src, 0, LAST_INDEX_OF(src, '.')) AS source, SUBSTRING(dst, 0, LAST_INDEX_OF(dst,'.')) AS destination;
iprecords = GROUP filter_input BY source;
totals = FOREACH iprecords {
		src = DISTINCT filter_input.source;
		dst = DISTINCT filter_input.destination;
		GENERATE FLATTEN( src ) AS source, COUNT( dst ) AS dest_count;
	}
top10 = LIMIT (ORDER totals BY dest_count DESC) 10;
-- dump totals;
STORE top10 INTO '/scr/rvshah/lab6/exp2/output/';
