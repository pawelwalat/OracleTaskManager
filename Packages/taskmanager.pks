CREATE OR REPLACE PACKAGE 
taskmanager
IS
   gi_jobs          number := 4;
   gi_instance_id   number;

   PROCEDURE rcreateinstance(p_stoponerror varchar2 default 'Y');

   PROCEDURE rstartjobs(vi_count number default null);

   PROCEDURE RLOG (PV_V1    VARCHAR2,
                   PV_V2    VARCHAR2 DEFAULT NULL ,
                   PV_V3    VARCHAR2 DEFAULT NULL );

   PROCEDURE rstopjobs;

   PROCEDURE rjobmonitor (vi_instance_id number);

   PROCEDURE rfinish;
END;
/