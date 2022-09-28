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
