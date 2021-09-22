libname z "/scratch/kennesaw/";

/* Annual CRSP and Compustat data */
proc sql;
	create table hft_predata0 as 
	select * from z.hft_comp as a 
	inner join z.retvol as b
	on a.gvkey=b.gvkey
	and a.datadate = b.datadate;
quit;

/* Merge to analyst-forecast data */
	/*Generate IBES-CRSP link table*/
%iclink;

proc sql;
	create table ibes_crsp as 
	select a.*, b.permno
	from z.ibes as a
	inner join iclink as b
	on a.ticker = b.ticker
	and b.score<6;
quit;

/* drop unnecessary columns */
data ibes_crsp;
	set ibes_crsp;
	drop pre1date pre2date post1date post2date lfr_fcst_num lfr_fcst_den;
run;

/* Merge annual data */
proc sql;
	create table hft_predata1 as 
	select *
	from ibes_crsp as a
	inner join hft_predata0 as b
	on a.permno = b.permno
	and a.fpedats = b.datadate;
quit;

/* Merge divergence data at daily level */
proc sql;
	create table hft_predata2 as 
	select a.*, b.vol, b.ret, b.turn, b.mato, b.baspread, b.dto, b.suv, 
			b.predicted_turn, b.volatility, b.disp1, b.disp2
	from hft_predata1 as a
	left join z.divergence as b
	on a.permno = b.permno
	and a.anndats = b.date;
quit;

/* Merge analyst following at year-end level */
proc sql;
	create table hft_predata3 as 
	select a.*, b.analysts
	from hft_predata2 as a
	left join z.divergence as b
	on a.permno = b.permno
	and a.fpedats = b.date;
quit;

/* Calculate the lead analysts dummy */
proc sort data= hft_predata3;
	by ticker fpedats descending lfr_analyst anndats anntims;
run;

data hft_predata3;
	set hft_predata3;
	leadstat + 1;
	by ticker fpedats descending lfr_analyst;
	if first.fpedats then leadstat=1;
run;

data hft_predata3;
	set hft_predata3;
	if analysts < 5 then lead_analyst = .; 
		else if 5<= analysts <10 and leadstat=1 then lead_analyst = 1;
		else if 10<= analysts <15 and leadstat<=2 then lead_analyst = 1;
		else if 15<= analysts <20 and leadstat<=3 then lead_analyst = 1;
		else if 20<= analysts <25 and leadstat<=4 then lead_analyst = 1;
		else if 25<= analysts <30 and leadstat<=5 then lead_analyst = 1;
		else if 30<= analysts <35 and leadstat<=6 then lead_analyst = 1;
		else if 35<= analysts <40 and leadstat<=7 then lead_analyst = 1;
		else if analysts>=40 and leadstat<=8 then lead_analyst = 1;
	else lead_analyst=0;
run;

data z.hft_predata;
	set hft_predata3;
run;

	/*House Cleaning*/
proc datasets lib=work kill memtype=data nolist; run;
quit;
	
