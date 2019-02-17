# utl-merging-and-transposing-category-sums-back-onto-each-source-record
Merging and transposing category sums back onto each source record.
    Merging and transposing category sums back onto each source record

     INPUT

       ID   YEAR   SEX  PAY
        1     1     F    22
        1     1     F    53
        1     1     M    46
        1     1     M    33

      OUTPUT

       ID   YEAR    SEX  PAY   PAY_F        PAY_F
        1     1      F    22    75   22+53    79   46+33
        1     1      F    53    75            79
        1     1      M    46    75            79
        1     1      M    33    75            96


    Given 20 million observations and 1,900 combinations of ID x Year,
    transppose the sums of by sex and merge the sums onto each source record.

    The original source for my feeble programming her is Paul Dorfmans post
    https://listserv.uga.edu/cgi-bin/wa?A2=SAS-L;ec7a990a.1902c
    and copied to the end of this post.
    Paul Dorfman
    sashole@bellsouth.net

    github
    https://tinyurl.com/y2xk2bq4
    https://github.com/rogerjdeangelis/utl-merging-and-transposing-category-sums-back-onto-each-source-record

    This can be useful for analytical tables where sums are needed.


     FOUR SOLUTIONS (All were batch runs Fastest to slowest)
     ======================================================

           Benchmarks(Batch)
           Seconds

       #4   10.12   Sasfile(1.49) proc report(2.52 core hog) Hash(6.11)  (proc summary/means cannot easily replace proc report?)
       #3   13.91   HASH Solution
       #2   18.59   SQL Join with subquery to compute sums
       #1   19.05   SQL self merge (no sub query)



    INPUT (20 million obs and 1,900 ID x YEAR combinations)
    =======================================================

    libname sd1 "d:/sd1";
    data sd1.have ;
      call streaminit(1234);
      do _n_ = 1 to 2e7 ;
        ID = ceil (ranuni(1) * 100) ;
        AorB = char ("AB", ceil (ranuni(1) * 2)) ;
        Year = 2000 + ceil (ranuni(1) * 19) ;
        Supply = chooseN (ceil (ranuni(1) * 3), 30, 60, 90) ;
        output ;
      end ;
      array ballast [10] (0 1 2 3 4 5 6 7 8 9) ;
    run ;

    WORK.HAVE total obs=10,000,000

            OBS  ID  AORB YEAR  SUPPLY BALLAST1 BALLAST2 BALLAST3 ...  BALLAST8 BALLAST9 BALLAST10

              1  19   B   2008    30       0        1        2    ...     7        8        9
              2  93   B   2011    60       0        1        2    ...     7        8        9
              3   5   A   2016    60       0        1        2    ...     7        8        9
           ....  ..   .   ....
      9.999.998  50   B   2001    30       0        1        2    ...     7        8        9
      9,999,999   8   A   2005    60       0        1        2    ...     7        8        9
     10,000,000  71   A   2012    90       0        1        2    ...     7        8        9

    EXAMPLE OUTPUT
    ==============

    WORK.HAVE total obs=10,000,000
                                           YEAR_      YEAR_     RULES                         Original Individual Data
            OBS  ID YEAR  SUPPLY   AORB   SUPPLY_A   SUPPLY_B   -----          -----------------------------------------------------------
                                                                              BALLAST1 BALLAST2 BALLAST3 ...  BALLAST8 BALLAST9 BALLAST10

              1  19 2008    30      B     306300     316320    sum of supply      0        1        2    ...     7        8        9
                                                     ------=>  when AorB=B
                                                               ID=19 Year=2008
              2  93 2011    60      B     306300     316320
              3   5 2016    60      A     314850     310470                       0        1        2    ...     7        8        9
           ....  .. ....                                                          0        1        2    ...     7        8        9
           ....  .. ....
           ....  .. ....            .
     19.999.998  50 2001    30      B     153390     153240                       0        1        2    ...     7        8        9
     19,999,999   8 2005    60      A     156390     161670                       0        1        2    ...     7        8        9
     20,000,000  71 2012    90      A     162720     155700                       0        1        2    ...     7        8        9


    Variable      Levels
    --------------------
    ID               100
    YEAR              19


    * BATCH MACRO;
    %macro sinbat(prg);
       filename sin pipe "sas -sysin d:/log/&prg..sas -log d:/log/&prg..log -config \cfg\sasv9.cfg -sasautos c:/oto";
       data _null_;
         infile sin;
         input;
         put _infile_;
       run;quit;
    %mend sinbat;

    SOLUTIONS
    =========

    ---------------------------------
    1. SQL self merge (Batch Run)
    ---------------------------------

    filename ft15f001 "d:/log/sqlSlfMrg.sas";
    parmcards4;
    libname sd1 "d:/sd1";
    proc sql;
      create table sqlSlfMrg as
      select *
           , sum (case when AorB = "A" then Supply else 0 end) as Year_Supply_A
           , sum (case when AorB = "B" then Supply else 0 end) as Year_Supply_B
      from   sd1.have
      group  ID, Year
    ;quit;run;
    proc sort  data=sqlSlfMrg(where=((id =19 and year=2008) or (id =93 and year=2011) or (id =5 and year=2016)))
      out=verify noequals nodupkey;
    by id year;
    run;quit;
    proc print data=verify(drop=balla:);
    run;quit;
    ;;;;
    run;quit;

    %sinbat(sqlSlfMrg);

    NOTE: The query requires remerging summary statistics back with the original data.
    NOTE: Table WORK.WANT_SQL0 created, with 20000000 rows and 16 columns.

    9        !  quit;
    NOTE: PROCEDURE SQL used (Total process time):
          real time           19.05 seconds
          cpu time            24.94 seconds

    Verification

                                       YEAR_       YEAR_
     ID    AORB    YEAR    SUPPLY    SUPPLY_A    SUPPLY_B

      5     A      2016      30       325530      321630
     19     B      2008      30       315450      318630
     93     B      2011      90       318480      312960


    -------------------------------------------
    2.  SQL Join with subquery to compute sums
    ---------------------------------------------

    filename ft15f001 "d:/log/sqlSubQue.sas";
    parmcards4;
    libname sd1 "d:/sd1";
    proc sql stimer _method ;
      create table sqlSubQue as
      select x.*
           , y.Year_Supply_A
           , y.Year_Supply_B
      from   sd1.have x
           , (select ID, Year
                   , sum ((AorB = "A") * Supply) as Year_Supply_A
                   , sum ((AorB = "B") * Supply) as Year_Supply_B
              from   sd1.have
              group  ID, Year) y
      where  x.ID = y.ID and x.Year = y.Year
      ;
    quit ;
    proc sort  data=sqlSubQue(where=((id =19 and year=2008) or (id =93 and year=2011) or (id =5 and year=2016)))
      out=verify noequals nodupkey;
    by id year;
    run;quit;
    proc print data=verify(drop=balla:);
    run;quit;
    ;;;;
    run;quit;

    %sinbat(sqlSubQue);

    NOTE: SAS Institute Inc., SAS Campus Drive, Cary, NC USA 27513-2414
    NOTE: The SAS System used:
          real time           18.59 seconds
          cpu time            24.36 seconds


    Verification

                                       YEAR_       YEAR_
     ID    AORB    YEAR    SUPPLY    SUPPLY_A    SUPPLY_B

      5     A      2016      30       325530      321630
     19     B      2008      30       315450      318630
     93     B      2011      90       318480      312960


    -----------------
    3. HASH Solution
    ----------------

    filename ft15f001 "d:/log/hash.sas";
    parmcards4;
    libname sd1 "d:/sd1";
    data hash ;
      if _n_ = 1 then do ;
        dcl hash h (ordered:"A", hashexp:20) ;
        h.definekey ("ID", "Year") ;
        h.definedata ("Year_Supply_A", "Year_Supply_B") ;
        h.definedone () ;
        do until (z) ;
          set sd1.have (drop = ballast:) end = z ;
          if h.find() ne 0 then call missing (Year_Supply_A, Year_Supply_B) ;
          Year_Supply_A + Supply * (AorB = "A") ;
          Year_Supply_B + Supply * (AorB = "B") ;
          h.replace() ;
        end ;
      end ;
      set sd1.have ;
      h.find() ;
    run ;
    proc print data=verify(obs=3 drop=balla:);
    run;quit;
    ;;;;
    run;quit;

    %sinbat(hash);


    NOTE: There were 20000000 observations read from the data set SD1.HAVE.
    NOTE: There were 20000000 observations read from the data set SD1.HAVE.
    NOTE: The data set WORK.HASH has 20000000 observations and 16 variables.
    NOTE: DATA statement used (Total process time):
          real time           13.91 seconds
          cpu time            13.46 seconds

    Verification  (the sort dedup changes the order)

                                       YEAR_       YEAR_
     ID    AORB    YEAR    SUPPLY    SUPPLY_A    SUPPLY_B

     19     B      2008      30       315450      318630
     93     B      2011      90       318480      312960
      5     A      2016      30       325530      321630


    -------------------------------
    4. Sasfile proc report hash
    -------------------------------

    filename ft15f001 "d:/log/rptHsh.sas";
    parmcards4;
    libname sd1 "d:/sd1";
    * I don't think summary or means can do this directly;
    %let beg=%sysfunc(time());
    sasfile sd1.have load;
    ods exclude all;
    proc report data=sd1.have nowd missing
       out=rptOut (sortedby=id year rename=(_c3_=supply_a _c4_=supply_B)
            drop=_break_) ;
    cols id year aorb, supply ;
    define id / group;
    define year / group;
    define aorb / across;
    define supply / sum;
    run;quit;
    ods select all;

    data rpthsh(drop=_rc);
      set sd1.have;
      if _n_=1 then
        do;
          if 0 then set rptOut;
          dcl hash h(dataset:'rptOut');
          _rc=h.defineKey('id','year');
          _rc=h.defineData(all:'y');
          _rc=h.defineDone();
        end;
      if h.find(key:id,key:year)=0 then output;
    run;quit;
    %put %sysevalf(%sysfunc(time()) - &beg);

    proc print data=rpthsh(obs=3 drop=balla:);
    run;quit;
    ;;;;
    run;quit;

    %sinbat(rptHsh);

    VERIFICATION

     Obs    ID    AORB    YEAR    SUPPLY    SUPPLY_A    SUPPLY_B

       1    19     B      2008      30       315450      318630
       2    93     B      2011      60       318480      312960
       3     5     A      2016      60       325530      321630

     NOTE: Libref SD1 was successfully assigned as follows:
          Engine:        V9
          Physical Name: d:\sd1
    2          * I don't think summary or means can do this directly;
    3          %let beg=%sysfunc(time());
    4          sasfile sd1.have load;
    NOTE: The file SD1.HAVE.DATA has been loaded into memory by the SASFILE statement.
    5          ods exclude all;
    6          proc report data=sd1.have nowd missing
    7             out=rptOut (sortedby=id year rename=(_c3_=supply_a _c4_=supply_B)
    8                  drop=_break_) ;
    9          cols id year aorb, supply ;
    10         define id / group;
    11         define year / group;
    12         define aorb / across;
    13         define supply / sum;
    14         run;

    NOTE: There were 20000000 observations read from the data set SD1.HAVE.
    NOTE: The data set WORK.RPTOUT has 1900 observations and 4 variables.
    NOTE: PROCEDURE REPORT used (Total process time):
          real time           2.52 seconds
          cpu time            8.20 seconds


    14       !     quit;
    15         ods select all;
    16         data rpthsh(drop=_rc);
    17           set sd1.have;
    18           if _n_=1 then

    2                                          The SAS System          23:29 Saturday, February 16, 2019

    19             do;
    20               if 0 then set rptOut;
    21               dcl hash h(dataset:'rptOut');
    22               _rc=h.defineKey('id','year');
    23               _rc=h.defineData(all:'y');
    24               _rc=h.defineDone();
    25             end;
    26           if h.find(key:id,key:year)=0 then output;
    27         run;

    NOTE: There were 1900 observations read from the data set WORK.RPTOUT.
    NOTE: There were 20000000 observations read from the data set SD1.HAVE.
    NOTE: The data set WORK.RPTHSH has 20000000 observations and 16 variables.
    NOTE: DATA statement used (Total process time):
          real time           6.11 seconds
          cpu time            5.60 seconds


    27       !     quit;
    28         %put %sysevalf(%sysfunc(time()) - &beg);
    10.1189999580965
    29         proc print data=rpthsh(obs=3 drop=balla:);
    30         run;

    NOTE: There were 3 observations read from the data set WORK.RPTHSH.
    NOTE: The PROCEDURE PRINT printed page 1.
    NOTE: PROCEDURE PRINT used (Total process time):
          real time           0.10 seconds
          cpu time            0.01 seconds


    30       !     quit;

    NOTE: SAS Institute Inc., SAS Campus Drive, Cary, NC USA 27513-2414
    NOTE: The SAS System used:
          real time           10.33 seconds
          cpu time            15.34 seconds



    *____             _
    |  _ \ __ _ _   _| |
    | |_) / _` | | | | |
    |  __/ (_| | |_| | |
    |_|   \__,_|\__,_|_|

    ;

    Surely it's nice - simple, quick to code, and leaving to do all the programming thinking to
    the whims of the SQL optimizer. And, as we know, PGStats is very good.

    However, we also know that nothing comes without a price. Algorithmically, it's obvious that
    no matter how you slice or dice it, this task cannot be accomplished in fewer than 2 passes
    through the file, even if the input file is already sorted.

    If it is sorted and SQL knows it - or we prompt it via the (SORTEDBY=ID Year) data set option,
    it aggregates on the first pass and merges on the second - the action no different, in principle,
    from the double DoW loop doing the same. Both are about equally fast, though SQL enjoys more code simplicity.

    If the input file is not sorted, the picture is different: SQL first sorts by [ID,Year] behind
    the scenes, aggregates the sorted file, and then merges. One side effect of this action is that
    the output file comes out sorted by [ID,Year] - which might be positive if this effect is
    wanted but quite negative and resource-wasting if the output is expected in the same intrinsic order as
     the input.

    If the latter is the case (i.e. we want the same order in the output as in the input), the
    SQL query can be reformulated to satisfy the need at the expense of some extra code. SQL
    will still choose to sort in order to aggregate, but will use a
    hash to merge the results back with the input on the second pass.

    On the other hand, a natural vehicle for this kind of task is using the DATA
    step to (1) accumulate the aggregates in a hash table on the first pass and (2)
    use the same table to look up the aggregated values by the key on the second pass.

    To see how these three approaches fare in comparison, let's concoct a more or
    less sizable input data set with enough ballast to render sorting (implicit or explicit)
    less palatable. This data set has 10M records and 14 variables:

    data have ;
      do _n_ = 1 to 1e7 ;
        ID = ceil (ranuni(1) * 100) ;
        AorB = char ("AB", ceil (ranuni(1) * 2)) ;
        Year = 2000 + ceil (ranuni(1) * 19) ;
        Supply = chooseN (ceil (ranuni(1) * 3), 30, 60, 90) ;
        output ;
      end ;
      array ballast [10] (0 1 2 3 4 5 6 7 8 9) ;
    run ;

    Now let's do:

    1. SQL via self-merge (as proposed by PGStat)
    2. SQL by way of a 2-step aggregate+merge to keep the input record order intact
    3. DATA step hash

    proc sql stimer _method magic=103;
      create table want_sql0 as
      select *
           , sum (case when AorB = "A" then Supply else 0 end) as Year_Supply_A
           , sum (case when AorB = "B" then Supply else 0 end) as Year_Supply_B
      from   have
      group  ID, Year
      ;quit;run;

    proc sql stimer _method ;
      create table want_sql1 as
      select *
           , sum (case when AorB = "A" then Supply else 0 end) as Year_Supply_A
           , sum (case when AorB = "B" then Supply else 0 end) as Year_Supply_B
      from   have
      group  ID, Year
      ;quit;run;

    proc sql stimer _method ;
      create table want_sql2 as
      select x.*
           , y.Year_Supply_A
           , y.Year_Supply_B
      from   have x
           , (select ID, Year
                   , sum ((AorB = "A") * Supply) as Year_Supply_A
                   , sum ((AorB = "B") * Supply) as Year_Supply_B
              from   have
              group  ID, Year) y
      where  x.ID = y.ID and x.Year = y.Year
      ;
    quit ;

    data want_hash ;
      if _n_ = 1 then do ;
        dcl hash h (ordered:"A", hashexp:20) ;
        h.definekey ("ID", "Year") ;
        h.definedata ("Year_Supply_A", "Year_Supply_B") ;
        h.definedone () ;
        do until (z) ;
          set have (drop = ballast:) end = z ;
          if h.find() ne 0 then call missing (Year_Supply_A, Year_Supply_B) ;
          Year_Supply_A + Supply * (AorB = "A") ;
          Year_Supply_B + Supply * (AorB = "B") ;
          h.replace() ;
        end ;
      end ;
      set have ;
      h.find() ;
    run ;

    I've compared the outputs and verified that they are all the same (except, of course, WANT_SQL1
    coming out sorted, rather that in the input order). Then I _NULL_-ified all the outputs and reran
    everything, as I was interested in comparing the underlying algorithms sans the times needed to
    write the outputs. Here's how the methods fared:

    Real time (seconds):

    SQL1: 9.6
    SQL2: 8.9
    Hash: 5.4

    Memory usage (megabytes):

    SQL1: 1028
    SQL2: 680
    Hash: 38

    It's kind of ironic that the hash object, while universally considered a "memory hog", in this
    case gets there 30-50 percent faster at the expense of 4 to 6 percent the SQL's memory consumption.
    The reason, of course, is that SQL gulps memory by leaps and bounds to expedite the sorting.

    And it's double ironic that SQL2 actually does use a[n internal] hash table: This is what the _METHOD
    option reports in the log as the SQXJHSH access method. The problem is that instead of (a) using the
    hash table for aggregation and (b) using the same table to tag the aggregates back to the input records
    (as it is done in the DATA step), SQL2 (a) aggregates via sorting (with its h
    uge memory-grabbing overhead) and (b) uses its hash table only to pair the aggregates
    to the original records on the second pass.

    When the input data volume is insignificant, none of the above matters, and the simplest code wins.
    However, in the world of big data and lots of people using an enterprise server concurrently,
    it can spell the difference between its being alive or going down. In such cases, it may make
    sense to encapsulate a piece of DATA step hash code similar to the above in a macro or user-defi
    ne call routine and suggest that it be used in specific cases instead of throwing data blindly at
    SQL in hope that it would sort the things out all by its internal wisdom. As we've seen above,
    it would sort them, indeed, only in a sense quite different from intended.

    Best regards


