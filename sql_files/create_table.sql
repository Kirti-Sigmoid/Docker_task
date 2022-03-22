create table if not exists dag_table as
 select dag_id, execution_date from dag_run order by execution_date
