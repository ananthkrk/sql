-- Historical CPU Utilization for last 48 hours
-------------------------------------------------
with 
ts as
(
    select  systimestamp - 4 st_tm,
            systimestamp     en_tm
    from    dual
),
osstat as
(
    select  to_char(s.begin_interval_time, 	'YYYY-MM-DD HH24:Mi')    snap_st,
            to_char(s.end_interval_time, 	'YYYY-MM-DD HH24:Mi')    snap_en,
            s.snap_id,
            s.instance_number   inst_id,
            os1.value           busy,
            os1.value - lag(os1.value) over (partition by os1.dbid, os1.instance_number order by s.snap_id) busy_delta,
            os2.value           idle,
            os2.value - lag(os2.value) over (partition by os2.dbid, os2.instance_number order by s.snap_id) idle_delta
    from    ts
            inner join dba_hist_snapshot s on s.begin_interval_time >= ts.st_tm and s.end_interval_time <= ts.en_tm
            inner join dba_hist_osstat os1 on s.snap_id = os1.snap_id and s.dbid = os1.dbid and s.instance_number = os1.instance_number and os1.stat_name = 'BUSY_TIME'
            inner join dba_hist_osstat os2 on s.snap_id = os2.snap_id and s.dbid = os2.dbid and s.instance_number = os2.instance_number and os2.stat_name = 'IDLE_TIME'
),
cpu as
(
    select  snap_id, snap_st, snap_en,
            inst_id,
            round(busy_delta*100/(busy_delta + idle_delta), 2) cpu_util
    from    osstat
)
select  snap_id, snap_st, snap_en, Node1, Node2
from    cpu
pivot
(
    sum(cpu_util)
    for inst_id in (1 as Node1, 2 as Node2)
)
order by snap_id desc;

