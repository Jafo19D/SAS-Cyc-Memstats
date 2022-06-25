/* Get usage data and cycle day from monthly NPV file, created by CUMLOAD_V3.sas */

/* Same as cyc_memstats_v3 except that it gives cycle 32s new cycles and assigns supertype */
   
   
proc print data=in1.cyv3&y.&m. (obs=10);
run;

proc contents data=in1.cyv3&y.&m.;
run;   
   
   
data u_&y.&m.(drop=annfee billadj ckgsrcnt commsur credit monthfee resubmit totcnt usgcnt surcre 
                   t_sess tsec );
  set in1.cyv3&y.&m. (rename=(accno=acct_num pricidx=price_in cyclenum=cycle totsec=tsec sessions=t_sess)
                       keep=accno cyclenum  service_ busid PRICIDX YRMO_NUM access sessions totsec 
					   bus_dt CANDATE CAN_CODE REACTDT  
					   annfee billadj ckgsrcnt commsur credit monthfee resubmit totcnt usgcnt surcre);

  if busid=6 and service_=2 and access=2;

  if price_in=400 then delete;										

  							
  by acct_num access bus_dt;
  retain  ;
  if first.access then do; 
      ann_fee=0;
      billadj_cent=0;
	  checksur_cent=0;
	  commsur_cent=0;
      credit_cent=0;
      monthfee_cent=0; 
	  resubmit_cent=0;
      total_cent=0;
	  usage_cent=0; 
      surcharge_credit=0;
      tot_sec=0; 
      sessions=0; 
  end;

  ann_fee+annfee;
  billadj_cent+billadj;
  checksur_cent+ckgsrcnt;
  commsur_cent+commsur;
  credit_cent+credit;
  monthfee_cent+monthfee;
  resubmit_cent+resubmit;
  total_cent+totcnt;
  usage_cent+usgcnt;
  surcharge_credit+surcre;
  tot_sec+tsec;
  sessions+t_sess;

  if last.access;
  
run;

proc print data=u_&y.&m. (obs=10);
run;

/* Merge u_&y.&m data with price_index table to get pi_descriptions */

proc sql;
connect to odbc(dsn=RB_DWREP02_AGG user=shibusawao password=shibusawao06);
create table price_index as select * from connection to odbc(
        select price_index as price_in, price_index_desc as pindex
        from price_index
		where bid=6
);
quit;

proc freq data=price_index;
  tables pindex;
run;

proc sort data=u_&y.&m.;
  by price_in;
run;

proc sort nodupkey data=price_index;
  by  price_in;
run;

proc print data=price_index;
run;


data u_&y.&m.;
  merge u_&y.&m.(in=a) price_index(in=b);
  by price_in;
  if a;
  
  if a and not b then pindex='Other';
  
  pindex2=pindex;
run;

proc freq data=u_&y.&m.;
  tables pindex pindex2;
run;



data u_&y.&m.;
  merge u_&y.&m.(in=a) price_index(in=b);
  by price_in;

  
  if a and not b then pindex='Other';
  
  pindex2=pindex;
  
  if a;
  
  if pindex not in ('5hr/\980/Std','Unltd/\1980/Std','1hr/\280/Std','Unltd/\980/Prov','5hr/\480/Prv',
                    '15/\1900/Std','ADSL type1 4150','12hr/\1480/Std','UUP 1year','Unlimited/\ 1039/ Ac',
					'ADSL type 1 3480','Komi Komi 10 Hours','Komi Komi 3 Hours',"Flets ADSL 1980",
					"Flets ADSL 1650",'ADSL 12M type1 4150', "Flets 3M free 1980","All-in-one ADSL 12M ",
					"Standard ADSL 12M ka","B Flet's Plan","Flet's ADSL Plan","All-in-one ADSL 24M"
					"ADSL 12M type2 5900","ADSL type2 5900","Flet's ADSL Plan Kar","Flet's ADSL Plan 1650",
					"Flet's ADSL Plan 165", "Flets Campaign", "B Flets Plan Kari","All-in-one ADSL 40M",
					"All-in-one ADSL 8M T","Std Unlimited Plan 2","Prov 5hr Plan 504yen","Prov Unlimited Plan",
                    "Gakuwari Plan 1040ye","Komi Komi 10hr Plan","Komi Komi 3hr Plan 1","Nenwari Plan 1871yen",
                    "Std 12hr Plan 1554ye","Std 1hr Plan 294yen","Std 5hr Plan 1029yen","UUP Berlitz") 
                    then pindex='Other';
					
  if index(upcase(pindex2),'STANDARD ADSL 12M') > 0 then pindex='Standard ADSL 12M';
  else if index(upcase(pindex2),'STANDARD ADSL 24M') > 0 then pindex='Standard ADSL 24M'; 
  else if index(upcase(pindex2),'STANDARD ADSL 40M') > 0 then pindex='Standard ADSL 40M'; 
  else if index(upcase(pindex2),"B FLETS'S") > 0 then pindex="B Flets"; 
  else if index(upcase(pindex2),"B FLET'S") > 0 then pindex="B Flets"; 
  else if index(upcase(pindex2),"B FLETS") > 0 then pindex="B Flets"; 
run;

proc sort data=u_&y.&m.;
  by acct_num;
run;

proc freq data=u_&y.&m.;
  tables pindex pindex2;
run;


/* Merge usage and member data and correct cycle date */

data cmv4&y.&m.(keep=acct_num cyclage num hours usage status offer pindex pindex2 price_in org_pi p_age promo 
					 act_days regday sessions payment cycle regdt regdt2 age37 bad
					 ann_fee billadj_cent checksur_cent commsur_cent credit_cent monthfee_cent resubmit_cent
					 total_cent usage_cent surcharge_credit) 
     missage;
  
  format usage $12. pindex pindex2 $35. payment $26. regdt2 mmddyy8.;
  
  merge u_&y.&m.(in=b) 
        in1.acctlist(rename=(accno=acct_num) keep=accno regdt promo offer inactive  
                     promo paytype paymeth member_status l_candt l_react cycle org_pi in=a);
   by acct_num;
   if a and b;
   
   if promo=494365 then delete; /* This is a test promo */

   if paytype='B' then payment='Debit Card';
     else if paytype='C' then payment='Credit Card';
     else if paytype='D' then payment='Direct Debit/Checking Acct';
     else if paytype='I' then payment='Invoice';
     else if paytype='N' then payment='Non-Pay';
     else payment='Other';
  
   if paymeth='PHONE2' then payment='Phone2';
     else if paymeth='PHONE' then payment='Phone';

/*  Determine member status by examining cancellation and reactivation dates */

   if candate = . then m_stat2="A";
     else if candate < reactdt then m_stat2="A";
     else if reactdt=. or reactdt < candate then do;
       m_stat2 = put(can_code,c&y.&m.c.); 
       if m_stat2 = '' then m_stat2='X';
     end;  
  
   status=m_stat2;
   
   if (l_candt gt l_react and member_status in ('A','P')) then status='I';
  
/*Determine Cyclge Age: 

  1) Extract month and year from YRMO_NUM     
  2) Calculate value for ACEND: for people who ended the cycle active or invalid,
           the end date should be the last day of the cycle, otherwise the end date is
           DATEPART(CANDATE)
  3) Call AGECALC3 macro */      
            
   m=mod(yrmo_num,100);
   y=int(yrmo_num/100);
   
  if cycle=32 then cycle=day(datepart(regdt-36000));
  
  regdt2=datepart(regdt-36000);

  if m in (1,3,5,7,8,10,12) then cend=mdy(m,31,y);
  else if m in (4,6,9,11) then cend=mdy(m,30,y);
  else if m=2 then cend=mdy(m,28,y);
     
  cyclage=intck('month',regdt2,cend);
  
  if cycle=1 then cyclage=cyclage+1;
  
  if month(regdt2)=m and year(regdt2)=y and cycle ne 1 then bad='y';
  else bad='n';

/* P_AGE is used in subsequent medain age processing
   Remove possible y2k testing accounts */
            
   p_age=cyclage;
   if p_age<0 then delete;
   
   if cyclage>=25 then cyclage=25;
   
   if p_age le 36 then age37=p_age;
   else if p_age gt 36 then age37=37;

     
/* Convert from cents to dollars */
      
   hours=tot_sec/3600;
   ann_fee=ann_fee/1.05;
   billadj_cent=billadj_cent/1.05;
   checksur_cent=checksur_cent/1.05;
   commsur_cent=commsur_cent/1.05;
   monthfee_cent=monthfee_cent/1.05;
   resubmit_cent=resubmit_cent/1.05;
   total_cent=total_cent/1.05;
   usage_cent=usage_cent/1.05;
   **surcharge_credit=surcharge_credit/1.05;
   
/* Classify hours into usage buckets */

   if hours=0 then usage="Zero";
     else if hours<5 then usage="less than 5";
     else if hours<10 then usage="5 - 10";
     else if hours<20 then usage="10 - 20";
     else if hours<30 then usage="20 - 30";
     else if hours<40 then usage="30 - 40";
     else if hours<50 then usage="40 - 50";
     else if hours<60 then usage="50 - 60";
     else if hours<70 then usage="60 - 70";
     else if hours<80 then usage="70 - 80";
     else if hours<90 then usage="80 - 90";
     else if hours<100 then usage="90 - 100";
     else if hours<110 then usage="100 - 110";
     else if hours<120 then usage="110 - 120";
     else if hours<130 then usage="120 - 130";
     else if hours<140 then usage="130 - 140";
     else if hours<150 then usage="140 - 150";
     else if hours>=150 then usage="150+";
     else put acct_num tot_sec hours;

   
/* NUM is a counting flag used during the reporting process */

   Num=1; 

/* Calculate the number of active days */
      
   select (m);                              
      when (2) eomday=28;            
      when (4,6,9,11) eomday=30;
        otherwise eomday=31;
   end;
           
   regday=day(datepart(regdt));        
   if regday>eomday then regday=eomday;
   
   if status='A' then act_days=(mdy(&m,regday,&y)-datepart(regdt)-inactive); 
   else if status^='A' and candate ^=. then act_days=((round((candate-regdt)/3600/24))-inactive);
   else act_days=(mdy(&m,regday,&y)-datepart(regdt)-inactive);

/* Output accounts with no cycle age to MISSAGE, otherwise retain in monthly cycstats file */
      
   if cyclage ne . then output cmv4&y.&m.;
     else output missage;
run;

proc print data=missage (obs=10);
   title4 'MISSING CYCAGE';
   run;
  
proc sql;
   drop table u_&y.&m.;  
   quit;

proc freq data=cmv4&y&m;
   tables price_in*offer*pindex/list missing;
   title 'freq';
   run;
   
proc print data=cmv4&y&m (obs=10);
title 'test';
run;   

*****************BRING IN SUPERTYPES**********;

proc sort data=cmv4&y.&m.;
  by promo;
run;

data _null_;
  format dt mmddyy8. ;
  call symput('dt',"'" || put(today()-30,mmddyy8.) || "'") ;

run;


proc sql;
connect to odbc(dsn=DW2_agg user=shibusawao password=shibusawao06);
create table maxdt as select * from connection to odbc(
select max(bus_dt) as maxdt
from PROMOTRAK_DAILY
where BUSINESS_ID = 6  and
      bus_dt >= &dt
);
quit;

data _null_;
  set maxdt;
  format maxdt mmddyy8. ;
  call symput('maxdt',"'" || put(maxdt,mmddyy8.) || "'") ;

run;

proc sql;
connect to odbc(dsn=DW2_agg user=shibusawao password=shibusawao06);
create table jpdm as select * from connection to odbc(
select P.DROP_MTH as dropm, p.promo_num as promo, pt.PROMOTRAK_TYPE_GROUP_LONG_DESC as descr, pd.bus_dt, 
       pt.PROMOTRAK_TYPE_LONG_DESC  as type
from PROMOTRAK_TYPE pt, PROMOTRAK_DAILY pd, PROMOTRAK p
where pd.bus_dt=&maxdt and 
      pd.BUSINESS_ID = 6 and
	  pd.PROMOTRAK_TYPE_ID = pt.PROMOTRAK_TYPE_ID and
	  p.Promo_num=pd.promo_num 
);
quit;


proc sort data=jpdm;
  by promo bus_dt;
run;


data last ;
  set jpdm ;
   by promo bus_dt;
   if last.promo;
   if descr='DoCoMo' and type='Hojin' then descr=type;
run;

data in.cmv4&y.&m.;
  merge cmv4&y.&m. (in=a) last (in=b keep=promo descr TYPE);
  by promo;
  if a;
  if descr=' ' then do;
    if promo=191951 then descr='AOLi';
	else descr='Unk';
  end;
  if type=' ' then do;
    if promo=191951 then type='AOLi';
	else type='Unk';
  end;
  
run;

proc freq data=in.cmv4&y.&m.;
  tables descr pindex cycle cyclage bad;
run;

data sample;
  set in.cmv4&y.&m.;
  if mod(_n_, 2000)=0;
run;

proc print data=sample;
  var acct_num regdt2 cycle p_age ann_fee billadj_cent checksur_cent commsur_cent credit_cent monthfee_cent 
      resubmit_cent total_cent usage_cent surcharge_credit;
run;

