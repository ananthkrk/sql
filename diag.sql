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







for i in 1..tb.count loop
if tb.exists (i-1) then
1 dff := 1 dff Il 'delta' litb(i).runnm||tb(i).runid|l' val > 0 or sel
It' val
1 dtl
sum (nvl ('|1 tb(i) .runnm||tb(j) .runidll' _val,0)) - sum(nV1 (' |1 tb(i-1) .runnm/Itb(i-1) .runid||' _val, 0)) as 'I| 'delta' Nitb(i).runnm|Itb(i).runid
1 hdr
1 dtl 11 'Ipad (delta' | Itb (i) .runnm| (tb(i) .runid||' _val'll ,25)111;
else
1 har Il substr (Lpad ('delta'| Itb (i) .runnm| Itb(j) . runid, 26) ,1, length (1pad ('delta' IItb (i) . runnm| Itb (i) runid, 26))-1);
1 sel := 1 sel
1 dtl
1 hdr end if;
1 pvt end loop;
'sum (nvl (' |1 tb(i) .runnm| Itb(i) .runidl|' val,0)) as "Iltb(i) .runnm| Itb(i) .runidll' _val';
'Apad (' litb (i) .runna| Itb(i) . runidll' val"ll? ,25) 1P':
:- 1 hdr il substr (1pad (tb (i) . zunnm| Itb (i) . runid,26) ,1, length (1pad (tb(i) . runnmI Itb(i) -runid, 26))-1) ;
1pvt 11 tb(i) ranidll" as "Iltb(i) .runnnlItb(i).runidli
close cr;
1_pvt
1 del
substr (1 _pvt, 1,length (1_pvt) -1) substr(1_dtl,1,length(1 dtl)-2):
1 dff substr (2_dtf, 1, length (2 atFs-):
wnr
L whr it
• and ('11 l def Il')'
1F pSUM -
ry' then
query else
- 1 query I| 1_sel Il ' from run stats pivot ( sum(stat_val) as vat 'I| 1 pvt I/') ) where stat_type in
('11 1 inpll ") 'Il' group by stat_type
1_query :- 1 query I| 1_gel Il ' from run stats pivot ( sum(stat_val) as val 'I| 1 pvt Il')) where stat_type = '11 chr (39) |1 pwhrIl chr (39) ||' group by stat_type,
stat name end 1f;
po_header:- 1 _hdr;
--dbms_output.put line (1 dtlIl ' txt from ('|| 1 query (1'y open po_cursor for
1 whr);
end;
1 dtl 11 " txt from ('ll 1 query Il*) ' 11 1_whr;
procedure rs start is bedin
select distinct sid into p sid from v$mystat; execute immediate 'truncate table run stats':
insert into run_stats (runid, runnm, stat_type, stat_name, stat_val)
select 0, 'BEFORE', "STATISTIC' a.name name, b.value from vSstatname a, vmystat b where
union all
a.statistich b.statistic:
selecu, union a11
'BEFORE', "LATCH' ,name, gets from v$latch
select U,
1 BEFORFI
, 'TIMER', 'hsecs!
name, hsecs from v$timer
union al select u.
"BEFORE;
TINE
union all
stat_name name, value from vSsess_time_model where sid a p sid
select 0,
'BEFORE', "WAIT'
event name
time waited micro from VSSESSION_EVENT where sid - P_sid
start
= dbms_utility.get cpu


Pg3


9_start:= dbms_utility.get cpu time;
end;
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
