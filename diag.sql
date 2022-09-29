set feedback off
set serveroutput on size unlimited ;
declare
	g_start		number;
	g_run		number;
	v_count		number;
	p_sid		number;
	rw_count	number;
	v_test		number;
	
	procedure buildqry (pwhr in varchar2, pSum in CHAR DEFAULT 'N', po_cursor out SYS_refcursor, po_header out LONG)
	is
	
	l_sel		LONG;
	l_query		LONG;
	l_hdr		LONG;
	l_dtl		LONG;
	l_pvt		LONG;
	l_whr		LONG;
	l_dff		LONG;
	s_sel		LONG;
	s_crq		LONG;
	s_hdr		LONG;
	l_inp		LONG;
	
	cursor cr is select distinct runid, runnm from run_stats order by runid;
	type rec is record
	(
		runid	number,
		runnm	varchar2(35)
	);
	type tbl is table of rec;
	tb   tbl;
	
	begin
		if pSum == 'Y' then
			l_hdr 	:= RPAD('SUMMARY .....', 80);
			l_inp 	:= chr(39)|| REPLACE(pWhr, ',', CHR(39)||','||CHR(39))||CHR(39);
			l_query := 'select stat_type, ';
			l_dtl 	:= 'select rpad(stat_type, 80)||';
		else
			l_hdr 	:= RPAD('DETAILS .....', 80);
			l_query := 'select stat_type, ';
			l_dtl 	:= 'select rpad(stat_type, 80)||';
		end if;
		
		open cr;
		fetch cr bulk collect into tb;
		for i in 1..tb.count loop
			if tb.exists (i-1) then
				l_dff := l_dff || 'delta' || tb(i).runnm||tb(i).runid||' val > 0'; 
				l_sel := l_sel || ', sum(nvl(' || tb(i).runnm || tb(i).runid || '_val, 0 )) - sum(nvl(' || tb(i-1).runnm || tb(i-1).runid || '_val, 0 )) as '|| 'delta' || tb(i).runnm ||  tb(i).runid || '_val';
				l_dtl := l_dtl || 'lpad(delta)' ||  tb(i).runnm || tb(i).runid || '_val'||',25)||';
				l_hdr := l_hdr || substr(lpad('delta'|| tb(i).runnm || tb(i).runid, 26), 1, length(lpad('delta'|| tb(i).runnm || tb(i).runid, 26))-1); 
			else
				l_sel := l_sel || ', sum(nvl(' || tb(i).runnm || tb(i).runid || '_val, 0 )) as '|| 'delta' || tb(i).runnm ||  tb(i).runid || '_val';
				l_dtl := l_dtl || 'lpad(delta)' ||  tb(i).runnm || tb(i).runid || '_val'||',25)||';
				l_hdr := l_hdr || substr(lpad('delta'|| tb(i).runnm || tb(i).runid, 26), 1, length(lpad('delta'|| tb(i).runnm || tb(i).runid, 26))-1); 								
			end if;
			l_pvt := l_pvt || tb(i).runid || ' as ' || tb(i).runnm || tb(i).runid || ',';
		end loop;
		close cr;
		
		1_pvt :=  substr(l_pvt, 1, length(l_pvt) - 1);
		l_dtl :=  substr(l_dtl, 1, length(l_dtl) - 1);
		l_dff :=  substr(l_dff, 1, length(l_dff) - 1);
		l_whr :=  l_whr || ' and ('|| l_dff ||') ';
		
		if pSUM = 'Y' then
			l_query :=  l_query  || l_sel || ' from run_stats puvot ( sum(stat_val) as val ' || l_pvt || ') ) where  stat_type in ('|| l_inp || ')' || ' group by stat_type ';
		else
			l_query :=  l_query  || l_sel || ' from run_stats puvot ( sum(stat_val) as val ' || l_pvt || ') ) where  stat_type = ' || chr(39) || pwhr || chr(39) || ' group by stat_type ';
		end if;
		po_hdr  := l_hdr;
		open po_cursor for l_dtl || ' txt from ( '|| l_query ||' ) ' || l_whr;
	end;
	procedure rs start 
	is 
	begin
	
		select distinct sid into p_sid from v$mystat; 
		execute immediate 'truncate table run stats':
		insert into run_stats (runid, runnm, stat_type, stat_name, stat_val)
		(
			select 0, 'BEFORE', 'STATISTIC' a.name name, b.value from vSstatname a, vmystat b where a.statistic# b.statistic#
			union all
			select 0, 'BEFORE', 'LATCH' ,name, gets from v$latch
			union all
			select 0, 'BEFORE', 'TIMER' ,'hsecs' name, hsecs from v$v$timer
			union all
			select 0, 'BEFORE', 'TIME' ,stat_name name, value from v$sess_time_model where sid = p_sid
			union all
			select 0, 'BEFORE', 'WAIT' ,event name , time_waited_micro from v$session_event where sid = p_sid
		);
		commit;
		g_start := dbms_utility.get_cpu_time;
		end;



Pg3


procedure rs middle (pstep in varchar2)
is
1 runid number:
begin
select max (runid) +1 into 1 runid from run stats;
select distinct sid into p sid from v$mystat;
insert into run stats (runid, runnn, stat_ type, stat _name, stat_val)
(
select 1 runid, pstep, 'STATISTIC', a.name name, b.value from vSstatname a, vSmystat b where a statistict = b.statistict
select _runid, pstep,
'LATCH', name, gets from v$latch
union all
select 1_runid, pstep, 'TIMER'
'hsecs' name, hsecs fron v$timer
union all
select 1 _runid, pstep, 'TIME',
•stat name demo.
value from vSsess_time model where sid - P_sid
union all
select 1 _runid, pstep, 'WAIT* event name, time waited_micro from V$SESSION_EVENT where sid - p_sid
);
COMMIT;
end;
procedure rs_stop is
1 cursor sys refcursor;
1 x
LONG;
1 header LONG;
begin
buildry ('TIMER, STATISTIC, LATCH, WAIT', '7', 1_CUrSOr, 1 header) ; dbms_output.put line (1 header);
loop
fetch 1 cursor into 1 x; exit when 1 cursortnotfound; dbms_output.put_line (1_x);
end loop;
close 1 cursor;
dbms_output.put_line (chr (9)) ;
buildgry ('STATISTIC', 'N' ,1 _CUrSOr, 1 _header) dibms output.put line (1 header);
Toor
fetch 1 cursor into 1 x; exit when 1 cursortnotfound;
dibms output.put line (1 x);
end loop:
dbms_ output.put_line (chr (9));
Pg4
]
Loop fetch 1 cursor into 1 x;
exit when 1 cursor&not found;
dbms output.put line (1 x)
end loop;
close 1 cursor;
doms_output.put_line (chr (9)) ;
buildgry ('WAIT'
'N' ,1 _cursor,
1 header):
dbms output.put line (1 header)
loop
fetch 1 cursor into 1 x;
exit when 1 cursor&not found;
dbms_output.put_ line (1 x)
end loop:
close l cursor;
dbms_output.put line (chr (9))
buildary ('TIME'
, 'N'.,l cursor, 1 header)
doms _output.put line (1 header)
loop
fetch 1 cursor into 1 x;
exit when 1 cursor notfound;
dbms_ output.put line (1x)
end loop:
close 1 cursor;
dbms_output.put_line (chr (9)) ;
白
buildgry ('LATCH'
'N'1 cursor,
dbms_output.put.
line (1 header)
Loop
fetch 1 cursor into 1 x; exit when I cursortnotfound;
dbms_ output.put line (1x)
end loop;
close l cursor;
dbms_output.put line (chr (9)) ;
end;
1 header)
3
4
5
6
Ebegin dbms_output.enable (buffer size -> NULL) :
ew_count : = 0;
rs start () ;
select count (*) into rw_count from MDXODS_CORE. LOAN;
rs middle (' LOAN' ) ;
select count (*) into rw count from MDXODS_CORE.SUBJECTPROPERTY:
rs middle (' SUBPROP');
rs stop ();
end;
