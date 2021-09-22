/* ********************************************************************************* */
/* ************** W R D S   R E S E A R C H   A P P L I C A T I O N S ************** */
/* ********************************************************************************* */

/* Summary: Calculate various proxies for divergence of investors' opinion           */
/*          - unexplained volume                                                     */
/*          - stock return volatility                                                */
/*          - bid ask spread                                                         */
/*          - analyst forecast dispersion                                            */
/* Date: May 2010                                                                    */
/* Author: Denys Glushkov, WRDS                                                      */
/* Details:                                                                          */
/*       - INPUT table containing the list of CRSP Permno identifiers                */
/*       - CRSP Daily Stock File (DSF), Daily Events File (DSE),                     */
/*       - IBES Detail Estimates (DET), Excluded and Stopped estimates, IBES ID file */
/*                                                                                   */
/* OUTPUT: DIVERGENCE dataset with various differences of opinion proxies            */
/* ********************************************************************************* */

 %let estwindow=60; /*estimation window for unexplained volume and volatility        */
 %let lag=7;        /*parameter used in unexplained volume de-trending               */
 %let gap=5;        /*parameter used for standardized unexplained volume calculation */
 %let begdate=01jan1990; /*beginning date of divergence of opinion proxy calculation */
 %let enddate=31dec2021; /*ending date of divergence of opinion proxy calculation    */
 %let stocks_filter=shrcd in (10,11); /*restrict to common stocks only               */
 %let exch_filter=exchcd in (1,2); /*NYSE/AMEX exchange filter                       */
 %let domain=epsus; /*use IBES files with EPS measure for US firms                   */

 /*IBES variables required for dipersion of analyst forecasts calculation*/
 %let ibes_vars=ticker fpedats fpi anndats actdats revdats analys measure value usfirm anndats_act estimator;
 %let ibes_filter="&begdate"d<=fpedats<="&enddate"d;

 %let dsevars=shrcd exchcd;                    /*CRSP event file variables           */
 %let dsfvars = vol ret shrout cfacpr cfacshr; /*CRSP stock file variables           */
 libname home '/scratch/kennesaw/';                             /*home directory on WRDS Unix Server  */
   

options sasautos=('/wrds/wrdsmacros/', SASAUTOS) MAUTOSOURCE; /*WRDS Research Macros */

 /* STEP 1: MERGE CRSP STOCK AND EVENTS DATA                                         */
 /* Merge CRSP stock and event files into the output file named "CRSP_D"             */
%CrspMerge(s=d,start=&begdate,end=&enddate,sfvars=&dsfvars,sevars=&dsevars,filters=&exch_filter and &stocks_filter);
    
/*INPUT table contains the list of distinct CRSP stock identifiers (PERMNO) for     */
/*which divergence of opinion proxies are to be calculated                          */
data input;
	set home.retvol;
	keep permno;
run;

proc sort data=input nodupkey; by permno; run;

/*STEP 2:CONSTRUCT PROXIES FOR OPINION DIVERGENCE */
/*Calculate market-wide turnover across NYSE/AMEX common stocks*/
Proc Sql;
 create view Market_Turn
  as select a.date, sum(vol*cfacshr)/sum(shrout*cfacshr*1000)
           as market_turn format=percent7.4
  from CRSP_D a where &exch_filter
  group by date
  order by date;
/*Firm-Specific turnover measure PERMNOs in user-created INPUT table*/
 create view Vol
  as select a.*, a.vol/(a.shrout*1000) as turn format=percent7.4, c.exchcd
  from Crsp.Dsf (keep=permno date ret vol shrout bidlo askhi bid ask) a,
       Input b,
       CRSP_D c
  where a.permno=b.permno and a.permno=c.permno and a.date=c.date
  order by date;
quit;
    
/*Calculate market-adjusted turnover and bid-ask spread             */
/*Adjust volume for NASDAQ stocks following Anderson and Dyl (2005) */
Data Volume; merge Vol Market_Turn (keep=date market_turn);
 by date;
 where date between "&begdate"d and "&enddate"d;
  if exchcd=3 then
  turn=(date <='01jan1997'd)*0.5*turn+(date>'01jan1997'd)*0.62*turn;
  mato=turn-market_turn; *adjust for market turnover;
  midpoint=coalesce(mean(ask,bid),mean(askhi,bidlo));
  baspread=coalesce(ask-bid, askhi-bidlo)/midpoint;
  format mato percent7.4;
  if not missing(permno);
  drop market_turn;
run;

/*sanity check; sort should produce no duplicates*/
Proc Sort Data=Volume nodupkey; by permno date;run;

/*Next step is detrending. Calculate moving 180-day median market-adjusted turnover*/
/*Any missing values of MATO will be replaced with the moving statistic while      */
/*leaving non-missing values unchanged                                             */
Proc Expand data=Volume out=Volume1 method=none;
 by permno; id date;
 convert mato=mato_median/transformout=(missonly movmed 180);
quit;

/*Unexplained volume: version 1                        */
/*See Garfinkel(2009), page 1325-1326                  */
Data Volume1; set Volume1;
 by permno date;
  mato_med_control=lag&lag(mato_median);
  if permno ne lag&lag(permno) then mato_med_control=.;
  dto=mato-mato_med_control;
  retpos=(ret>0)*abs(ret);
  retneg=(ret<0 and not missing(ret))*abs(ret);
  format dto percent7.4 date date9.;
  drop mato_median mato_med_control;
  label turn='Daily Turnover'
        mato='Daily Market-Adjusted Turnover'
        dto='Change in Market-Adjusted Turnover'
        baspread='Bid-Ask Spread';
run;

Proc Sort Data=volume1 out=volume2 nodupkey; by permno date; run;

/*Unexplained volume: version 2 (standardized unexplained volume)               */
/*See Garfinkel (2009), page 1326-1327                                          */
/*Create trading calendar based on the length of estimation window (estwindow), */
/*and the trading day gap between the end of estimation period and the date     */
/*of the actual unexplained volume calculation. Using trading calendar ensures  */
/*that the same number of trading days is used in calculations                  */
Data _Caldates;
     merge Crsp.Dsi (keep=date rename=(date=estper_beg))
     Crsp.Dsi (keep=date firstobs=%eval(&estwindow) rename=(date=estper_end))
     Crsp.Dsi (keep=date firstobs=%eval(&estwindow+&gap+1));
     format estper_beg estper_end date date9.;
     if missing(estper_beg)=0 and missing(estper_end)=0 and missing(date)=0;
run;
 
/*Start of the countdown for rolling regressions*/
proc sql noprint;
    create table Start as 
    select a.date, abs(a.estper_beg-b.first_date) as dist, b.last_date format=date9.
    from _Caldates a, (
            select min(date) as first_date, max(date) as last_date
            from Volume2 (where=(not missing(turn)))) b
    having dist=min(dist)
    order by date desc;
    select date format=8., last_date format=8. into: k_start,
                                                   : k_end
    from start (firstobs=1);
quit;

/*Starting and ending trading days for the rolling regressions module required     */
/*to calculate stock return volatility and standardized unexplained volume         */
%put 'Starting Date For Rolling Regressions '; %put %sysfunc(putn(&k_start,date9.));

%put 'Ending Date For Rolling Regressions ';   %put %sysfunc(putn(&k_end,date9.));

options nosource nonotes;
filename junk dummy; proc printto log = junk; run;

%Macro REGS;
 %do k=&k_start %to &k_end;
 /*read the trading days for the beginning and the end of the estimation period*/
  data _Null_; set _Caldates (where=(date=&k));
    call symput('start',estper_beg);
    call symput('end',estper_end);
  run;

  
  proc reg data=Volume2 noprint edf outest=params;
    by permno;
    where &start <=date <=&end;
 /*the overstatement of volume for NASDAQ securities to be captured by intercept*/
    model turn=retpos retneg;
    model ret=;
  quit;

  data Params; set Params;
   date=&k; format date date9.;
  run;
  proc append base=Params_all data=Params;run;
 %end;
%mend;

%REGS;
options source notes;

proc printto;run;
  
Proc Sort Data=Params_all thread; by permno date;run;

/*Compute standardized unexplained volume and keep only those       */
/*observations for which missing turnover values do not exceed 20%  */
/*of the estimation window                                          */
Data Suv;
 merge Volume2 (in=a) Params_all (where=(upcase(_depvar_)='TURN')
  keep=permno _depvar_ date _rmse_ intercept retpos retneg _p_ _edf_
  rename=(retpos=retpos_beta retneg=retneg_beta));
 by permno date;
  predicted_turn=intercept+retpos_beta*retpos+retneg_beta*retneg;
  suv=(turn-predicted_turn)/_rmse_ ; /*standardized unexplained volume measure*/
  /*impose Chung and Zhang (2009) filters on Bid-Ask measure*/
  if ((bid=0 and ask=0) or (bidlo=0 and askhi=0))
      or abs(baspread/midpoint)>0.5
  then delete;
  if sum(_p_,_edf_)>=0.8*&estwindow and a;
  keep permno date ret vol turn suv mato dto predicted_turn baspread;
  format predicted_turn percent7.4 suv best5. baspread percent7.4;
  label predicted_turn='Predicted Turnover'
        suv='Standardized Unexplained Volume';
run;

/*STEP 2: CONSTRUCT ANALYST FORECAST DISPERSION PROXY*/
/*WRDS Links between CRSP and IBES*/
%ICLINK;

/*Define the universe of IBES Tickers*/
Proc Sql;
 create table _Sample
  as select distinct ticker, permno
  from Iclink
  where permno in (select distinct permno from input)
  and score in (0,1)
  order by permno;
/*select IBES tickers to compute the analyst forecast dispersion                */
/*Exclude erroneous observations for which Earnings Announcement Date (ANNDATS) */
/*precedes the Review Date (REVDATS)                                            */
create view _Temp
  as select a.*, b.permno, put(anndats, yymmn.) as yearmon
  from Ibes.Det_&domain (keep=&ibes_vars) a, _Sample b
  where a.ticker=b.ticker and nmiss(fpedats,anndats_act)=0
  and &ibes_filter and fpi='1' and a.anndats_act>a.revdats and analys ne 0
  order by ticker, fpedats, analys, yearmon, anndats, revdats;
quit;

/*keep only the latest stock forecasts by an analyst in a given month*/
Data _Temp1/view=_Temp1; set _Temp;
  by ticker fpedats analys yearmon anndats;
  if last.yearmon;
run;
   
Proc Sql;
/*Extract relevant stopped estimates*/
 create view Stopped
  as select a.ticker, a.fpedats, a.estimator, a.astpdats
  from Ibes.Stop_&domain a, _Sample b
  where a.pdicity='A' and &ibes_filter and
  a.ticker=b.ticker and not missing(a.astpdats)
  group by a.ticker, a.fpedats, a.estimator
  having astpdats=max(astpdats);
/*Extract relevant excluded estimates*/
 create view Excluded
  as select a.ticker,a.fpedats,a.actdats,a.estimator,a.analys,a.excdats,a.excends
  from Ibes.Exc_&domain a, _Sample b
  where &ibes_filter and fpi='1' and a.ticker=b.ticker and not missing(a.excdats);
/*Merge Detailed Estimates with Stopped and Excluded files*/
 create table _Temp2 (drop=measure fpi usfirm actdats)
  as select c.*, d.excdats, d.excends
  from (select a.*, b.astpdats from _Temp1 a left join Stopped b
  on a.ticker=b.ticker and a.estimator=b.estimator and a.fpedats=b.fpedats) c
  left join Excluded d
  on c.ticker=d.ticker and c.fpedats=d.fpedats and c.actdats=d.actdats and
  c.estimator=d.estimator and c.analys=d.analys;
quit;

Proc Sort Data=_Temp2 noduprec; by _all_;run;

/*Remove forecasts that were stopped or excluded*/
Data _Temp2; set _Temp2;
 if anndats>=astpdats and nmiss(anndats,astpdats)=0 then delete;
 if (nmiss(excdats,excends)=0 and excdats<=anndats<=excends)
 or (not missing(excdats) and anndats>=excdats) then delete;
 drop astpdats excdats excends;
run;

/* Sorting anndats and revdats in descending order is intentional to define leads  */
/* The Estimate will be carried forward to either the next estimate issue date,    */
/* Anndats(t+1), or Anndats(t)+105, or EAD, whichever comes sooner                 */
/* Ensure forecast dispersion measure doesn't include stale forecasts              */
Proc Sort Data=_Temp2 nodupkey;
 by ticker analys fpedats descending anndats descending revdats;
run;

data _Temp2; set _Temp2;
 by ticker analys fpedats descending anndats descending revdats;
 leadanndats=min(lag(anndats), intnx('day',anndats,105));
 if first.fpedats then leadanndats=min(intnx('day',anndats,105), anndats_act);
 leadanndats_me=intnx('month',leadanndats,0,'e');
 anndats_me=intnx('month',anndats,0,'e');
 format leadanndats date9. anndats date9. leadanndats_me date9. anndats_me date9.;
run;

/*Populate Detailed Forecasts into monthly frequency, so that for each    */
/*Ticker-Year-Month an analyst forecast dispersion measure can be defined */
Proc Sql;
 create view Base
  as select distinct b.ticker, intnx('month',b.date,0,'E') as date format=date9., intnx('month',b.date,1,'E') as lead_date format=date9.
  from (select distinct ticker, intnx('month',min(anndats),0,'B') as minfdate, intnx('month',max(leadanndats),0,'E') as maxfdate
        from _Temp2 group by ticker) a,
             (select * from (select distinct ticker from _Temp2),
             (select distinct date from Crsp.Msi)
             where "&begdate"d <= date <= "&enddate"d) b
             where a.ticker=b.ticker and a.minfdate<=b.date<=a.maxfdate
             order by b.ticker, date;
/*Carrying the forecasts forward until the next appropriate date (leadanndats) */
/*If the forecast is issued in the same month as earnings announcement,        */
/*use the imperfect inequality while populating into monthly frequency         */
 create table Dispersion
  as select *
  from Base a left join _Temp2 (drop=yearmon) b
  on a.ticker=b.ticker and
 ((b.anndats <= a.date < b.leadanndats)*(anndats_me < leadanndats_me)
  or
 (b.anndats<=a.date<=leadanndats_me and anndats_me=leadanndats_me))
 order by a.ticker, a.date, b.analys, b.fpedats, b.anndats;
quit;

/*Sanity Check. Should be no full duplicates*/
Proc Sort Data=Dispersion noduprec; by _all_;run;
    
/* For a given (Ticker,Year,Month,Analyst Forecast) combination, keep only those   */
/* records with the closest Fiscal Period End. Populate missing PERMNO             */
Proc Sort Data=Dispersion; by ticker date analys fpedats anndats;run;

Data Dispersion; set Dispersion;
 by  ticker date analys fpedats anndats;
 retain permno1;
 if not missing(permno) then permno1=permno;
 if first.ticker then permno1=permno;
 drop permno; rename permno1=permno;
 if first.analys;
run;

/*link in the CRSP price data. Construct mean monthly price approximation*/
Data Price/view=Price; set Crsp.Msf;
 by permno date;
 date=intnx('month',date,0,'E');
 mean_price=(abs(prc/cfacpr)+lag(abs(prc/cfacpr)))/2;
 if first.permno then mean_price=abs(prc/cfacpr);
 keep permno date mean_price;
run;    

/* Calculate two versions of analyst forecast dispersion: one scaled by absolute */
/* mean forecast value (DISP1), the other scaled by mean monthly price (DISP2)   */
Proc Sql;
 create table Disp_Final
  as select distinct a.ticker, a.date, a.lead_date, a.permno,
  count(distinct analys) as analysts
  label='Number of analysts with outstanding valid forecast as of prior month-end',
  std(a.value)/abs(mean(a.value)) as disp1
  label='Analyst Forecast Dispersion in prior month
        (scaled by absolute mean forecast)',
  std(a.value)/b.mean_price as disp2
  label='Analyst Forecast Dispersion in prior month (scaled by mean monthly price)'
  from Dispersion a left join Price b
  on a.permno=b.permno and a.date=b.date
 group by a.ticker, a.date
 order by a.ticker, a.date, analysts, disp1, disp2;
quit;    

/* Keep records with non-missing price data for ambiguous      */
/* IBES Ticker-CRSP Permno matches - those will be last record */
/* for a given (ticker-date) pair                              */
Data Disp_Final; set Disp_Final;
 by ticker date;
 if last.date;
run;

/*Put all DIVOP measures together*/
Proc Sql;
 create table Home.Divergence
  as select *
  from Suv c left join
   (select a.permno, a.date, a._rmse_ as volatility
    label='Daily Stock Volatility (STD) over the estimation period',
    b.ticker, b.analysts, b.disp1, b.disp2
    from Params_all
   (where=(upcase(_depvar_)='RET' and sum(_p_,_edf_)>= 0.8*&estwindow)) a
   left join Disp_Final b
   on a.permno=b.permno and put(a.date,yymmn.)=put(b.lead_date, yymmn.)) d
  on c.permno=d.permno and c.date=d.date
 order by c.permno, c.date;
quit;

/*House Cleaning*/
Proc Sql;
 drop table Disp_Final, Dispersion, _Temp2, _Sample, Suv, Params, Crsp_D, Params_all, Start, _Caldates, Volume, Volume1, Volume2;
 drop view  Vol,Price, Base, Stopped, Excluded, _Temp1, _Temp, Market_Turn;
quit;

/* *************  Material Copyright Wharton Research Data Services  *************** */
/* ****************************** All Rights Reserved ****************************** */
/* ********************************************************************************* */
