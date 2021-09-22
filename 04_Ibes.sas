data ibes0;
set ibes.det_epsus;
if year(fpedats)>1989;
if measure = "EPS";
if fpi = 6; /* One quarter ahead */
day_interval = intck('day', anndats, anndats_act);
run;

proc sort data=ibes0; by day_interval; run;

/* Identify 2 preceding and 2 following analyst forecasts for each forecast */
/* proc sql; */
/* 	create table lags */
/* 	as select ticker, fpedats, anndats as currentdate, */
/* 		lag(anndats, 1) over(order by fpedats, anndats) AS post1date, */
/* 		lag(anndats, 2) over(order by fpedats, anndats) AS post2date */
/* 	from ibes0; */
/* quit; */

/* Keep the first forecast for analyst-firm-fpedats triples */
proc sort data=ibes0 out=triples(keep=ticker fpedats analys anndats) nodupkey; 
	by ticker fpedats analys; 
run;

/* Keep the unique firm-forecast days for lead-lag identification */
proc sort data=triples out=uniques (keep=ticker anndats) nodupkey; 
	by ticker anndats; 
run;

/* Now, leads and lags for firm-forecast days */
proc expand data=uniques out=prepost (keep=ticker anndats pre1date pre2date post1date post2date);
	convert anndats = pre1date/transformout=(lag 1);
	convert anndats = pre2date/transformout=(lag 2);
	convert anndats = post1date/transformout=(lead 1);
	convert anndats = post2date/transformout=(lead 2);
	by ticker;
run;

/*Back to main data*/
proc sql;
	create table ibes1
	as select a.*, b.pre1date, b.pre2date, b.post1date, b.post2date
	from ibes0 as a
	left join prepost as b
	on a.ticker = b.ticker
	and a.anndats = b.anndats;
quit;

data ibes1;
	set ibes1;
	lfr_fcst = (intck('day', pre1date, anndats) + intck('day', pre2date, anndats))/(intck('day', anndats, post1date)+intck('day', anndats, post2date));
	lfr_fcst_num = intck('day', pre1date, anndats) + intck('day', pre2date, anndats);
	lfr_fcst_den = intck('day', anndats, post1date)+intck('day', anndats, post2date);
run;

/*Aggregate LFR numerators and denominators for past two years for each analyst-firm pair*/
proc sql;
	create table groups
	as select analys, ticker, fpedats, sum(lfr_fcst_num) as lfr_num, sum(lfr_fcst_den) as lfr_den, count(analys) as times
	from ibes1
	group by analys, ticker, fpedats
;
	
	create table lfr as
	select a.analys, a.ticker, a.fpedats, sum(a.lfr_num) as lfr_num, sum(a.lfr_den) as lfr_den, sum(a.times) as times
	from groups as a
	left join groups as b
	on a.analys = a.analys
	and a.ticker = b.ticker
	and 0<= year(a.fpedats) - year(b.fpedats) <= 1
	group by a.analys, a.ticker, a.fpedats
;
quit;

/* Merge back to IBES data */
proc sql;
	create table ibes2
	as select a.*, b.lfr_num/b.lfr_den as lfr_analyst, b.times
	from ibes1 as a
	left join groups as b
	on a.analys = b.analys
	and a.ticker = b.ticker
	and a.fpedats= b.fpedats;
quit;

libname z "/scratch/kennesaw/";
data z.ibes;
set ibes2;
run;

	/*House Cleaning*/
proc datasets lib=work kill memtype=data nolist; run;
quit;
