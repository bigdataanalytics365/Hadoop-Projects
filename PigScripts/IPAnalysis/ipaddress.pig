-- Load the data for network log.
file_input = LOAD '/class/s17419/lab6/network_trace' USING PigStorage(' ') AS (time:chararray, word:chararray, src:chararray, sign:chararray, dst:chararray, protocol:chararray, extra:chararray);
-- Filter out the TCP protocol records.
tcp_input = FILTER file_input BY protocol == 'tcp';
-- Reformat the ip addresses so that it does not have unnecessary information in it. Ecerytihin after the last '.'
filter_input = FOREACH tcp_input GENERATE SUBSTRING(src, 0, LAST_INDEX_OF(src, '.')) AS source, SUBSTRING(dst, 0, LAST_INDEX_OF(dst,'.')) AS destination;
-- Group the resulting relation by source ip address.
iprecords = GROUP filter_input BY source;
-- For each of the source ip addresss generate source ip and count of distinct destination address for that source ip address.
totals = FOREACH iprecords {
		src = DISTINCT filter_input.source;
		dst = DISTINCT filter_input.destination;
		GENERATE FLATTEN( src ) AS source, COUNT( dst ) AS dest_count;
	}
-- Order the resulting relation by the coune of destination ip addresses descending.
top10 = LIMIT (ORDER totals BY dest_count DESC) 10;
-- Print out the results in terminal.
-- dump top10;
-- Store the results in hdfs file.
STORE top10 INTO '/scr/rvshah/lab6/exp2/output/';
