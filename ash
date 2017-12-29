SELECT h.snap_id, h.instance_number, h.sample_time, h.sql_id, s.sql_text FROM DBA_HIST_SQLTEXT s join dba_hist_active_sess_history h on s.sql_id = h.sql_id
WHERE UPPER(s.SQL_TEXT) LIKE '%V_SUBJECTPROPERTYSTDLOAN%'
order by h.sample_time desc;

SELECT OUTPUT FROM TABLE( DBMS_WORKLOAD_REPOSITORY.AWR_SQL_REPORT_HTML( l_dbid     => 408857094,  
                                                                        l_inst_num => 3, 
                                                                        l_bid      => 72243, 
                                                                        l_eid      => 72353 , 
                                                                        l_sqlid    => '65sdvm0s9yv5q'
                                                                       )
                        );


--AWR SQL PLAN
---------------
 SELECT PLAN_HASH_VALUE, SQL_ID, ID, OPERATION, OPTIONS, OBJECT_NAME, OBJECT_OWNER, OBJECT_TYPE 
 FROM   DBA_HIST_SQL_PLAN
 WHERE  DBID = ? 
 AND    SQL_ID =  ?
 ORDER BY SQL_ID, PLAN_HASH_VALUE, ID


 WITH x AS 
 (
  SELECT INSTANCE_NUMBER, SNAP_ID, SAMPLE_TIME, SQL_ID, SQL_PLAN_HASH_VALUE,
  LAG(SQL_PLAN_HASH_VALUE, 1) OVER (PARTITION BY DBID, INSTANCE_NUMBER ORDER BY SAMPLE_TIME) previous_plan_hash_value
  FROM DBA_HIST_ACTIVE_SESS_HISTORY
  WHERE DBID=? AND SQL_ID=?
  AND SNAP_ID BETWEEN ? AND ? 
 )
 SELECT instance_number inst#, snap_id, sample_time, sql_id, sql_plan_hash_value
 FROM x
 WHERE previous_plan_hash_value IS NULL OR previous_plan_hash_value!=sql_plan_hash_value
 ORDER BY sample_time, instance_number

AWR SQLStats for SQL_ID per Snapshot
----------------------------------
 WITH x AS 
 ( 
    SELECT DBID, INSTANCE_NUMBER, 
    LAG(SNAP_ID, 1) OVER (PARTITION BY DBID,INSTANCE_NUMBER ORDER BY SNAP_ID) begin_snap_id, 
    SNAP_ID end_snap_id, END_INTERVAL_TIME end_snap_time
    FROM DBA_HIST_SNAPSHOT WHERE DBID=? AND INSTANCE_NUMBER=? AND END_INTERVAL_TIME > SYSDATE - ?
 ) 
 SELECT y.INSTANCE_NUMBER inst#, x.begin_snap_id||'-'||x.end_snap_id snap_id, x.end_snap_time,
 y.SQL_ID, y.PLAN_HASH_VALUE, ROUND(y.ELAPSED_TIME_DELTA/1000000) elapsed_seconds, 
 ROUND(y.CPU_TIME_DELTA/1000000) cpu_seconds,
    y.ROWS_PROCESSED_DELTA rows_processed, y.BUFFER_GETS_DELTA buffer_gets, y.DISK_READS_DELTA  disk_reads, y.EXECUTIONS_DELTA  executions, y.PARSE_CALLS_DELTA parses
 FROM DBA_HIST_SQLSTAT y JOIN x ON (y.DBID=x.dbid AND y.INSTANCE_NUMBER=x.instance_number AND y.SNAP_ID=x.end_snap_id)
 WHERE x.begin_snap_id IS NOT NULL AND y.PLAN_HASH_VALUE=?
 ORDER BY x.begin_snap_id, x.end_snap_id, y.SQL_ID
 

AWR SQL Stats by SQL Plan Hash Value
--------------------------------------
 SELECT y.INSTANCE_NUMBER inst#, TRUNC(x.END_INTERVAL_TIME, 'HH24') hour, y.PLAN_HASH_VALUE,
 COUNT(DISTINCT y.SQL_ID) distinct_sql, COUNT(DISTINCT PARSING_SCHEMA_NAME) distinct_schema,
 ROUND(SUM(y.ELAPSED_TIME_DELTA) /1000000) elapsed_seconds, 
 ROUND(SUM(y.CPU_TIME_DELTA) /1000000) cpu_seconds,
    SUM(y.ROWS_PROCESSED_DELTA) rows_processed, SUM(y.BUFFER_GETS_DELTA) buffer_gets, SUM(y.DISK_READS_DELTA)  disk_reads, SUM(y.EXECUTIONS_DELTA)  executions, SUM(y.PARSE_CALLS_DELTA) parses
 FROM DBA_HIST_SQLSTAT y JOIN DBA_HIST_SNAPSHOT x ON 
 (y.DBID=x.DBID AND y.INSTANCE_NUMBER=x.INSTANCE_NUMBER AND y.SNAP_ID = x.SNAP_ID) 
 WHERE y.DBID=? AND y.INSTANCE_NUMBER=? AND x.END_INTERVAL_TIME> SYSDATE - ? AND y.PLAN_HASH_VALUE=? 
 GROUP BY y.INSTANCE_NUMBER, TRUNC(x.END_INTERVAL_TIME, 'HH24'), y.PLAN_HASH_VALUE
 ORDER BY TRUNC(x.END_INTERVAL_TIME, 'HH24') 

 
 
 AWR SQL Elapsed Time by SQL_ID
 -------------------------------
  WITH x AS ( 
     SELECT DBID, INSTANCE_NUMBER, 
     LAG(SNAP_ID, 1) OVER (PARTITION BY DBID,INSTANCE_NUMBER ORDER BY SNAP_ID) begin_snap_id, 
     SNAP_ID end_snap_id, END_INTERVAL_TIME end_snap_time
     FROM DBA_HIST_SNAPSHOT WHERE END_INTERVAL_TIME > SYSDATE - ?
  ) 
  SELECT y.INSTANCE_NUMBER inst#, x.begin_snap_id||'-'||x.end_snap_id snap_id, x.end_snap_time,
  y.SQL_ID, y.PLAN_HASH_VALUE, 
  ROUND(y.ELAPSED_TIME_DELTA/1000000) elapsed_seconds,
  ROUND(y.CPU_TIME_DELTA/1000000) cpu_seconds,
  ROUND(y.CLWAIT_DELTA/1000000) cluster_wait_seconds,
  ROUND(y.IOWAIT_DELTA/1000000) io_wait_seconds,
  ROUND(y.CCWAIT_DELTA/1000000) concurrency_wait_seconds,
  ROUND(y.APWAIT_DELTA/1000000) app_wait_seconds,
  ROUND(y.PLSEXEC_TIME_DELTA/1000000) plsql_exec_seconds,
  ROUND(y.JAVEXEC_TIME_DELTA/1000000) java_exec_seconds
  FROM DBA_HIST_SQLSTAT y JOIN x ON (y.DBID=x.dbid AND y.INSTANCE_NUMBER=x.instance_number AND y.SNAP_ID=x.end_snap_id)
  WHERE x.begin_snap_id IS NOT NULL AND y.DBID=? AND y.INSTANCE_NUMBER=? AND y.SQL_ID=?
 ORDER BY x.begin_snap_id, x.end_snap_id, y.PLAN_HASH_VALUE
