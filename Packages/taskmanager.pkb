CREATE OR REPLACE PACKAGE BODY
               taskmanager
IS
   PROCEDURE rcreateinstance(p_stoponerror varchar2 default 'Y')  
   IS
      vi_count   number;      
      vi_id number;
      vv_status taskmanager_instances.status%TYPE;
      TYPE cur IS REF CURSOR;
      v_cur  cur;
      v_p1 varchar2(4000);
      v_p2 varchar2(4000);
      v_p3 varchar2(4000);
      vv_tasksql   varchar2(4000);
      vv_tasksql2   varchar2(4000);
   BEGIN
      rlog ('TASKMANAGER', 'RCREATEINSTANCE', 'started');
      select nvl(max(id),0) into vi_id from taskmanager_instances;
            
      insert into taskmanager_instances (id, createdate, status, stoponerror) values (vi_id+1, sysdate, 'STARTED', p_stoponerror);

    gi_instance_id :=vi_id+1;

    select nvl(max(id),0) into vi_id from taskmanager_tasks;

    insert into taskmanager_tasks (id, instance_id, group_id, status,tasksql, tasksql2, createdate)
    select vi_id+1+rownum,gi_instance_id, group_id, 'NEW', tasksql, tasksql2, sysdate  from taskmanager_taskdefs where datasourcesql is null;
    
    for v in (select * from taskmanager_taskdefs where datasourcesql is not null) loop
        OPEN v_cur FOR v.datasourcesql;  
         LOOP
           FETCH v_cur INTO v_p1, v_p2, v_p3;
           vv_tasksql :=v.tasksql;
           vv_tasksql :=replace(vv_tasksql,'$p1$',v_p1);
           vv_tasksql :=replace(vv_tasksql,'$p2$',v_p2);
           vv_tasksql :=replace(vv_tasksql,'$p3$',v_p3);
           vv_tasksql2 :=v.tasksql2;
           vv_tasksql2 :=replace(vv_tasksql2,'$p1$',v_p1);
           vv_tasksql2 :=replace(vv_tasksql2,'$p2$',v_p2);
           vv_tasksql2 :=replace(vv_tasksql2,'$p3$',v_p3);

            insert into taskmanager_tasks (id, instance_id, group_id, status,tasksql, tasksql2)
            values
          ((select nvl(max(id),0)+1 from taskmanager_tasks),gi_instance_id, v.group_id, 'NEW', vv_tasksql, vv_tasksql2);  
         EXIT WHEN v_cur%NOTFOUND;
   END LOOP;
    CLOSE v_cur;          
    

    
    end loop;
                               
      rlog ('TASKMANAGER', 'RCREATEINSTANCE', 'end');
   END;

   PROCEDURE rstartjobs(vi_count number default null) -- can be used to resume
   IS
      vi_i       number := 0;
      vi_jobid   number;
   BEGIN
      IF gi_instance_id IS NULL
      THEN
         SELECT   MAX (id) INTO gi_instance_id FROM taskmanager_instances;
      END IF;
      
      update taskmanager_instances set startdate=sysdate where id=gi_instance_id and startdate is null; 
      update taskmanager_instances set resumedate=sysdate where id=gi_instance_id;
      
      if vi_count is not null then
        gi_jobs := vi_count;
      end if;        


      DBMS_OUTPUT.put_line('begin execute immediate ''begin TASKMANAGER.rjobmonitor('
                           || gi_instance_id
                           || '); end;''; end;');

      WHILE vi_i < gi_jobs
      LOOP
         SYS.DBMS_JOB.SUBMIT (
            job         => vi_jobid,
            what        => 'begin sleep( dbms_random.value(100,1000)); execute immediate ''begin TASKMANAGER.rjobmonitor('
                          || gi_instance_id
                          || '); end;''; end;',
            next_date   => SYSDATE + (1 + vi_i) / 24 / 60 / 60,
            interval    => 'SYSDATE+1/24/60/60',
            no_parse    => FALSE,
            instance    => MOD (vi_i, 2)
         );
         rlog ('TASKMANAGER',
               'RSTARTJOBS',
               'Job (' || vi_i || ')submited: ' || vi_jobid);
         vi_i := vi_i + 1;
         COMMIT;
      -- SYS.DBMS_JOB.RUN (vi_jobid, TRUE);
      END LOOP;
   END;

   PROCEDURE rstopjobs
   IS
   BEGIN
      IF gi_instance_id IS NULL
      THEN
         SELECT   MAX (id) INTO gi_instance_id FROM TASKMANAGER_INSTANCES;
      END IF;

      rlog ('TASKMANAGER', 'rstopjobs', 'Begin');

      FOR v
      IN (SELECT   *
            FROM   user_jobs
           WHERE   what LIKE
                      '%TASKMANAGER.rjobmonitor(%)%')
      LOOP
         BEGIN
            DBMS_JOB.REMOVE (v.job);
            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               NULL;
         END;
      END LOOP;
   END;
     

   PROCEDURE rjobmonitor (vi_instance_id number)
   IS
      vi_processinggroup_id   number;
      vi_id                   number;
      vv_sqlerrm              varchar2 (4000);
      vv_partitionname        varchar2 (100);
      vv_sql                  varchar2 (4000);
      vv_sql2                  varchar2 (4000);      
      vi_count                number;
      vv_log                  varchar (4000);
      vv_tablename            varchar2 (4000);
      vv_subpartition         varchar2 (4000);
      vv_retrytask            number;
      vv_retryinstance        number;
   BEGIN
      gi_instance_id := vi_instance_id;
      rlog ('TASKMANAGER', 'RJOBMONITOR', 'Begin');

      -- stop jobs if there was any errors on any DB and stoponerror=on
      SELECT   COUNT ( * )
        INTO   vi_count
        FROM   TASKMANAGER_TASKS t
       WHERE   status IN ('ERROR')
        and (select stoponerror from taskmanager_instances tmi where tmi.id=t.instance_id )='Y'
               AND instance_id = gi_instance_id;             

      SELECT   COUNT ( * )+vi_count
        INTO   vi_count
        FROM   TASKMANAGER_TASKS t
       WHERE   status IN ('ERROR')
            and (select stoponerror from taskmanager_instances tmi where tmi.id=t.instance_id )='Y'
            and not exists (select 1 from TASKMANAGER_TASKS t2 where t2.instance_id = gi_instance_id and t2.GROUP_ID=t.GROUP_ID and status in ('NEW','PROCESSING','WAITFORRETRY'))
               AND instance_id = gi_instance_id;  

      IF vi_count > 0
      THEN
         rstopjobs;
      END IF;

      --stop jobs if there are no more tasks
      SELECT   COUNT ( * )
        INTO   vi_count
        FROM   TASKMANAGER_TASKS t
       WHERE        status IN ('NEW', 'PROCESSING','WAITFORRETRY')
               AND instance_id = gi_instance_id;

      IF vi_count = 0
      THEN
         rstopjobs;
      END IF;

      BEGIN
         SELECT   MIN (GROUP_ID)
           INTO   vi_processinggroup_id
           FROM   TASKMANAGER_TASKS t
          WHERE        status IN ('NEW', 'PROCESSING','WAITFORRETRY')
                  AND instance_id = gi_instance_id;


             SELECT   id,
                      TASKSQL, 
                      TASKSQL2
               INTO   vi_id,
                      vv_sql,
                      vv_sql2
               FROM   TASKMANAGER_TASKS t
              WHERE    (status = 'NEW' or (status = 'WAITFORRETRY' and sysdate-enddate>1/24/60/2))
                      AND GROUP_ID = vi_processinggroup_id
                      AND ROWNUM = 1
                      AND instance_id = gi_instance_id
         FOR UPDATE   OF status NOWAIT;

         UPDATE   TASKMANAGER_TASKS
            SET   status = 'PROCESSING', resumedate = SYSDATE
          WHERE   id = vi_id;

         UPDATE   TASKMANAGER_TASKS
            SET   startdate = SYSDATE
          WHERE   id = vi_id and startdate is null;


         COMMIT;
         BEGIN
            --- start processing

            rlog ('TASKMANAGER', 'RJOBMONITOR', vv_sql);
            rlog ('TASKMANAGER', 'RJOBMONITOR', vv_sql2);

            EXECUTE IMMEDIATE vv_sql||' '||vv_sql2 USING OUT vv_log;

            --end processing
            UPDATE   TASKMANAGER_TASKS
               SET   status = 'COMPLETED', enddate = SYSDATE, LOG = vv_log
             WHERE   id = vi_id;

            COMMIT;

            --check if that was last task and remove jobs
            SELECT   COUNT ( * )
              INTO   vi_count
              FROM   TASKMANAGER_TASKS t
             WHERE       status IN ('NEW', 'PROCESSING','WAITFORRETRY')
                     AND instance_id = gi_instance_id;

            IF vi_count = 0
            THEN
               rstopjobs;
            END IF;


            
         EXCEPTION
            WHEN OTHERS
            THEN
               vv_sqlerrm := substr(SQLERRM,1,3999);

               select nvl(retrycount,0) into vv_retryinstance from taskmanager_instances 
               where id=gi_instance_id;
               
               select nvl(retry,0) into vv_retrytask from TASKMANAGER_TASKS
               where id=vi_id; 

                if vv_retrytask<vv_retryinstance then
                   UPDATE   TASKMANAGER_TASKS
                      SET   status = 'WAITFORRETRY', LOG = vv_sqlerrm,
                      retry=nvl(retry,0)+1, enddate=sysdate
                    WHERE   id = vi_id;                               
                else
                   UPDATE   TASKMANAGER_TASKS
                      SET   status = 'ERROR', LOG = substr(vv_sqlerrm||chr(10)||chr(13)||dbms_utility.format_error_backtrace,1,4000), enddate=sysdate
                    WHERE   id = vi_id;
                end if;

               rlog (
                  'TASKMANAGER',
                  'rjobmonitor',
                     'vi_processinggroup_id: '
                  || vi_processinggroup_id
                  || ' vi_id:'
                  || vi_id
                  || ' '
                  || substr(SQLERRM,1,3499)
               );
               COMMIT;
         END;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            --DBMS_LOCK.sleep (2);
            sleep(2*1000);
         WHEN OTHERS
         THEN
            rlog (
               'TASKMANAGER',
               'rjobmonitor',
                  'vi_processinggroup_id: '
               || vi_processinggroup_id
               || ' vi_id:'
               || vi_id
               || ' '
               || substr(SQLERRM,1,3499)
            );
            COMMIT;
      END;
      rlog ('TASKMANAGER', 'RJOBMONITOR', 'End');
   END;

   PROCEDURE RLOG (PV_V1    VARCHAR2,
                   PV_V2    VARCHAR2 DEFAULT NULL ,
                   PV_V3    VARCHAR2 DEFAULT NULL )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO TASKMANAGER_LOGS
        VALUES   (
                     SYSTIMESTAMP,
                     SUBSTR (PV_V1, 0, 4000),
                     SUBSTR (PV_V2, 0, 4000),
                     SUBSTR (PV_V3, 0, 4000),
                     SUBSTR (
                           'Instance: '
                        || gi_instance_id
                        || ' Job: '
                        || SYS_CONTEXT ('USERENV', 'BG_JOB_ID'),
                        0,
                        4000
                     )
                 );

      COMMIT;
   END;

   PROCEDURE rfinish
   IS
   BEGIN
      UPDATE   TASKMANAGER_INSTANCES
         SET   enddate = SYSDATE;
   END;

END;
/
