proc sql;
	create table base0
	as select CONSOL, COSTAT, CURCD,DATADATE, DATAFMT, FYEAR, GDWLIP, GVKEY, INDFMT, POPSRC, RCP, SETP, 
			SPIOP, WDP,	AT,	CEQ, CIK, CONM, CSHO, CUSIP, EXCHG,	FIC, FYR, IB, LT, OANCF, PRCC_F, PRCC_C, 
			SALE, SPI, TIC, SICH, OIBDP, DVC, OIADP, XRD, DLTT, ADRR, ACT,  LCT, CH, DD1, TXP, CAPX, DP, DV,
			WCAP, RE, PPENT, PPEGT, COGS, XIDOC, RECCH, XINT, IBC, RECT, INVT, DLC, XAD, AP, INTAN, RECD, AU, PI,
			TSTKC, PRSTKC, SSTK, DVPSX_F, AJEX, EPSPX, dpact, ebit, DVPSP_C, DVPSP_F, INTPN, NI, PSTKRV
		from compd.funda;
quit;

	/*Clean Data!!*/
data base1;
	set base0;
	if datafmt = "STD";/*Non restated data*/
	if indfmt = "INDL"; /*Industrial format*/
/* 	if fyear ne .; */
/* 	if at ne .; */

	/*Standard Variables*/
/*Size*/
	size = log(csho*prcc_f);
	logat = log(at);

/*Leverage*/
	lev1 = (DLTT/AT);
	lev2 = (DLTT+DLC)/(prcc_f*csho);
	da = LT/AT;

/*ROA & ROE*/
	roa = ib/at;
	roe = ib/ceq;

/*Market to Book*/
	mtb = ((at - ceq) + (prcc_f*csho))/at;	

run;

/*Sales Growth*/
	/*Create Lag Sales*/
data lags;
	set base1 (rename=(
				sale = lagsale
				));
	lagyear=fyear+1;
	keep gvkey lagyear lagsale;
run;

proc sql;
	create table base2
	as select * 
	from base1 as a, lags as b
	where a.gvkey=b.gvkey
	and a.fyear = b.lagyear;
quit;

data base2;
	set base2;
	salgrowth = (lagsale-sale)/lagsale;
run;

	/*Get Header SIC Codes from Company file*/
proc sql;
	create table base3
	as select a.*, b.sic from base2 as a
	left join  comp.names as b
	on a.gvkey=b.gvkey;
quit;

data base3;
	set base3;
	%FFI48(SIC);
run;

libname z "/scratch/kennesaw/";
data z.hft_comp;
	set base3;
	keep gvkey fyear datadate cik sic ffi48 
		size logat lev1 lev2 da roa roe mtb salgrowth;
run;

	/*House Cleaning*/
proc datasets lib=work kill memtype=data nolist; run;
quit;
