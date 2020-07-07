-- Scheduler Jobs
-----------------------
SELECT * from dba_scheduler_jobs                WHERE OWNER = 'SVNG';
select * from dba_scheduler_running_jobs        WHERE OWNER = 'SVNG';
select * from dba_SCHEDULER_JOB_LOG             WHERE OWNER = 'SVNG' order by log_date desc;
select * from dba_scheduler_job_run_details     WHERE OWNER = 'SVNG' order by actual_start_date desc;
select * from dba_scheduler_job_run_details     WHERE OWNER = 'SVNG' AND job_name = 'SVNG' order by actual_start_date desc;

-- Parallel Chunks
-------------------
select * from DBA_PARALLEL_EXECUTE_CHUNKS       where task_owner =  'SVNG' and task_name  = 'MVW_PERS' order by start_ts;
select * from DBA_PARALLEL_EXECUTE_CHUNKS       where task_owner =  'SVNG' and task_name  = 'MVW_PERS';
select * from DBA_PARALLEL_EXECUTE_TASKS        where task_owner =  'SVNG' and task_name  = 'MVW_PERS';

-- Status of Parallel Chunks
-------------------------------
select  task_owner, status, count(*) 
from    dba_parallel_execute_chunks 
where   task_name = 'MVW_PERS'
group by task_owner, status;

--- Generic Views
--------------------
select * from dba_ddl_locks where owner = 'SVNG';
select * from dba_objects   where owner = 'SVNG' and object_id = 1330502;
select * from dba_tables    where owner = 'SVNG' and degree <> 1;

select * from gv$active_session_history where user_id = 85 ;

select * from gv$active_session_history where session_id = 868 and session_serial# = 12894;
select * from gv$active_session_history where session_id = 117 and session_serial# = 7074;

select * from gv$active_session_history where sql_id            = 'gdzhg46gyz6qx';
select * from gv$active_session_history where top_level_sql_id  = '01sruwmk0c7w4';

select * from gv$active_session_history where module  = 'DBMS_SCHEDULER';

select * from gv$session                where inst_Id = 1 and sid = 466;
select * from gv$sql                    where sql_id = '01sruwmk0c7w4';
