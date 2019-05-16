Oracle Task Manager  - A simple tool to run parallel tasks Oracle Database with full control.

1. Quick Start

Download the latest release
Run install_taskmanager.sql script from SQL Plus (alternatively manually create objects from Procedures, Tables and Packages directories)
Create task definition
Start tasks execution

2.  Documentation

2.1. Tables

TASKMANAGER_INSTANCES - Instances of Taskmanager executions. Taskmanager is executing tasks from last instance, old instances are kept for evidence.
TASKMANAGER_TASKDEFS - List of task definitions, base on this definition new tasks are added when instance is created with the API.
TASKMANAGER_TASKS - List of all tasks, for all instances. Each task can be in status NEW, PROCESSING, COMPLETED or ERROR.
TASKMANAGER_LOGS - Logs from taskmanager executions.

2.2. Procedures

SLEEP - Procedure (in Java) created as a workarround, in case DBMS_LOCK.sleep is not available.

2.3 Configuration

When creating new instance following parameters might be set:
STOPONERROR - Indicates if all tasks should be stopped when error occur in one of the tasks. 
Possible values: 	Y - All running tasks will be completed, no new tasks should be started.
			N - All running and new tasks from current group will be started. No new tasks from next groups will be started.
RETRYCOUNT - Indicates how many times task should be retried in case of failure. (some specific tasks might fail when running in the same group in other tasks. When retrying few times they will complete successfully).					

2.3. API
All API procedures are available in the taskmanager package.

PROCEDURE rcreateinstance(p_stoponerror varchar2 default 'Y')  - creates new instance (TASKMANAGER_INSTANCES) and tasks (TASKMANAGER_TASKS) base on the definition from TASKMANAGER_TASKDEFS table.

PROCEDURE rstartjobs(vi_count number default null) - start jobs from last instance. vi_count parameter indicates number of parallel jobs to be started

PROCEDURE RLOG(text,text,text) - internal procedure for logging purposes. Can be executed from outside. Handle from 1 to 3 text parameters. Log entry is added to the TASKMANAGER_LOGS table.

PROCEDURE rstopjobs - Procedure to stop all jobs gracefully (no new jobs are started, all running jobs will complete)

PROCEDURE rjobmonitor (vi_instance_id number) - internal procedure, executed by oracle job.

PROCEDURE rfinish - internal procedure to close entry in TASKMANAGER_INSTANCES.

2.5. Examples

2.5.1 Refreshing all MVs on the schema in parallel

Step 1) Create task definition

Insert into DATAMART.TASKMANAGER_TASKDEFS
   (ID, GROUP_ID, TASKSQL, TASKSQL2, DATASOURCESQL)
 Values
   (1, 1, 'BEGIN
  DBMS_SNAPSHOT.REFRESH(
    LIST                 => ''$p1$''
   ,PUSH_DEFERRED_RPC    => TRUE
   ,REFRESH_AFTER_ERRORS => FALSE
   ,PURGE_OPTION         => 1
   ,PARALLELISM          => 0
   ,ATOMIC_REFRESH       => TRUE
   ,NESTED               => FALSE);
   :vvlog := ''OK'';
END;
', NULL, 
    'select mview_name, null, null from user_mviews');
COMMIT;

Step 2) Create taskmanager instance

begin
taskmanager.rcreateinstance('N');
end;
/

Step 3) Run tasks

begin
taskmanager.rstartjobs(8); --run 8 parallel jobs
end;
/

Step 4) Monitor tasks and wait till all are completed

select * from taskmanager_tasks where status = 'PROCESSING';

select * from taskmanager_tasks where status = 'NEW';

select * from taskmanager_tasks where status = 'COMPLETED';

select status, count(*) from taskmanager_tasks group by status;

4. Copyright and license
   Please check LICENSE file
