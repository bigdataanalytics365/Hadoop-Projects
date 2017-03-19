-- Firewall log format: <Time> <Connection ID> <Source IP> <Destination IP> “Blocked”
-- Load dat from two files
trace_file_input = LOAD '/class/s17419/lab6/ip_trace' USING PigStorage(' ') AS (time:chararray, cid:int, src:chararray, sign:chararray, dst:chararray, protocol:chararray, extra:chararray);
block_file_input = LOAD '/class/s17419/lab6/raw_block' USING PigStorage(' ') AS (cid:int, status:chararray);
-- Filter raw_block with by status == 'Blocked'
blocked_input = FILTER block_file_input BY status == 'Blocked';
-- Join the two by Connection id
joined_input = JOIN trace_file_input BY cid, blocked_input BY cid;
-- Select out necessary info.
records = FOREACH joined_input GENERATE trace_file_input::time, trace_file_input::cid, trace_file_input::src, trace_file_input::dst, blocked_input::status;
-- Store records into firewall file.
STORE records INTO '/scr/rvshah/lab6/exp3/firewall/';
-- Generate distinct source ip
source_records = FOREACH records GENERATE trace_file_input::src AS source, trace_file_input::dst AS destination;
iprecords = GROUP source_records BY source;
totals = FOREACH iprecords {
		src = DISTINCT source_records.source;
		GENERATE FLATTEN( src ) AS source, COUNT( source_records.destination ) AS dest_count;
	}
-- Order blocked ip by # of times they were blocked DESC.
blocked_ips = ORDER totals BY dest_count DESC;
-- Store all blocked ip in to output
STORE blocked_ips INTO '/scr/rvshah/lab6/exp3/output/';