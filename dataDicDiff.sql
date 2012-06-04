drop table if exists casebook_tables;
drop table if exists casebook_tables_and_columns;
drop table if exists casebook_tables_sorted;
drop table if exists casebook_tables_and_columns_sorted;
drop function if exists dataDicDiff();

create table casebook_tables_and_columns (
  table_name varchar(255),
  attribute_name varchar(255),
  datatype varchar(255),
  n_bytes integer default null,
  length integer default null,
  description text,
  foreign_key_links_to varchar(255) default null,
  configurable boolean default null,
  redundant_column boolean default 'f',
  change_type varchar(255) default null,
  change_detected_on date default null
);

create table casebook_tables (
  table_name varchar(255),
  description text,
  data_value_table boolean default 'f',
  change_type varchar(255) default null,
  change_detected_on date default null
);

create table casebook_tables_and_columns_sorted (
  table_name varchar(255),
  attribute_name varchar(255),
  datatype varchar(255),
  n_bytes integer default null,
  length integer default null,
  description text,
  foreign_key_links_to varchar(255) default null,
  configurable boolean default null,
  redundant_column boolean default 'f',
  change_type varchar(255) default null,
  change_detected_on date default null
);

create table casebook_tables_sorted (
  table_name varchar(255),
  description text,
  data_value_table boolean default 'f',
  change_type varchar(255) default null,
  change_detected_on date default null
);

--Load data from csv
copy casebook_tables from '/tmp/casebook_tables.csv' 
with delimiter as ',' 
CSV HEADER 
FORCE NOT NULL description;

copy casebook_tables_and_columns 
from '/tmp/casebook_tables_and_columns.csv' 
with delimiter as ',' 
CSV HEADER 
FORCE NOT NULL description; 

--Wrap the text descriptions within single quotes so that the commas within
--them do not split the descriptions
/*update casebook_tables_and_columns
set description = '"' || description || '"';

update casebook_tables
set description = '"' || description || '"';*/

create function dataDicDiff() returns void as $$
declare

  tables_columns_from_db record;
  tables_from_db record;

  tables_columns_from_dd record;
  tables_from_dd record;

  vCount integer;
  
  c_tables_columns_in_db cursor for  
  SELECT c.relname, a.attname, t.typname, 
         nullif(a.attlen, -1) AS length, 
         nullif(a.atttypmod, -1) AS lengthvar 
  FROM pg_class c, pg_attribute a, pg_type t
  WHERE c.relname !~ '^(pg_|sql_)' 
  AND c.relname !~ 'casebook_tables'
  AND c.relname <> 'casebook_tables_and_columns'
  AND c.relname <> 'casebook_tables_sorted'
  AND c.relname <> 'casebook_tables_and_columns_sorted'
  AND c.relname <> 'audit_log'
  AND c.relname <> 'schema_migrations'
  AND c.relkind = 'r'
  AND a.attnum > 0
  AND a.attrelid = c.oid
  AND a.atttypid = t.oid;
  
  c_tables_in_db cursor for
  SELECT c.relname FROM pg_class c 
  WHERE c.relname !~ '^(pg_|sql_)' 
  AND c.relkind = 'r'
  AND c.relname <> 'casebook_tables'
  AND c.relname <> 'casebook_tables_and_columns'
  AND c.relname <> 'casebook_tables_sorted'
  AND c.relname <> 'casebook_tables_and_columns_sorted'
  AND c.relname <> 'audit_log'
  AND c.relname <> 'schema_migrations';

  c_tables_columns_in_dd cursor for
  select * from casebook_tables_and_columns
  where change_type is NULL or change_type = 'added';

  c_tables_in_dd cursor for
  select * from casebook_tables
  where change_type is NULL or change_type = 'added';
  
begin

  --Detect columns which have been added. Should be present in metadata but 
  --not in data dictionary.
  
  for tables_columns_from_db in c_tables_columns_in_db loop
     
     vCount := 0;
     
     SELECT count(*) into vCount
     FROM casebook_tables_and_columns
     where table_name = tables_columns_from_db.relname
     and attribute_name = tables_columns_from_db.attname;

     if (vCount = 0) then
     
       raise notice 'Column % in table % has been added', 
         tables_columns_from_db.attname,
         tables_columns_from_db.relname;
       
       insert into casebook_tables_and_columns 
       (table_name, attribute_name, datatype,
        n_bytes, length,
        change_type, change_detected_on)
       values (tables_columns_from_db.relname, 
               tables_columns_from_db.attname,
               tables_columns_from_db.typname,
               tables_columns_from_db.length,
               tables_columns_from_db.lengthvar,
               'added',
               current_date);
     end if;

  end loop;

  --Detect tables which have been added. Should be present in metadata but 
  --not in data dictionary.
  
  for tables_from_db in c_tables_in_db loop
     
     vCount := 0;
     
     SELECT count(*) into vCount
     FROM casebook_tables
     where table_name = tables_from_db.relname;

     if (vCount = 0) then
     
       raise notice 'Table % has been added', 
         tables_from_db.relname;
       
       insert into casebook_tables
       (table_name, 
        change_type, change_detected_on)
       values (tables_from_db.relname,
               'added',
               current_date);
     end if;

  end loop;
  

  --Detect columns which have been dropped. Should be present in data dictionary but 
  --not in metadata.
  
  for tables_columns_from_dd in c_tables_columns_in_dd loop
     
     vCount := 0;
     
     SELECT count(*) into vCount
     FROM pg_class c, pg_attribute a, pg_type t
     WHERE c.relkind = 'r'
     AND a.attnum > 0
     AND a.attrelid = c.oid
     AND a.atttypid = t.oid
     and c.relname = tables_columns_from_dd.table_name
     and a.attname = tables_columns_from_dd.attribute_name;

     if (vCount = 0) then
     
       /*raise notice 'Column % in table % has been dropped', 
         tables_columns_from_dd.attribute_name,
         tables_columns_from_dd.table_name;*/
       
       update casebook_tables_and_columns 
       set change_type = 'dropped',
           change_detected_on = current_date
       where table_name = tables_columns_from_dd.table_name
       and attribute_name = tables_columns_from_dd.attribute_name;
     end if;

  end loop;
  --Detect tables which have been dropped. Should be present in data dictionary but 
  --not in metadata.
  
  for tables_from_dd in c_tables_in_dd loop
     
     vCount := 0;

     SELECT count(*) into vCount 
     FROM pg_class 
     WHERE relname = tables_from_dd.table_name
     AND relkind = 'r';

     if (vCount = 0) then

       --raise notice 'Table % has been dropped', tables_from_dd.table_name;
       
       update casebook_tables 
       set change_type = 'dropped',
           change_detected_on = current_date
       where table_name = tables_from_dd.table_name;
     end if;
     
    
  end loop;

  --Reload data in sorted form
  insert into casebook_tables_and_columns_sorted
  select * from casebook_tables_and_columns
  order by table_name, attribute_name;

  insert into casebook_tables_sorted
  select * from casebook_tables
  order by table_name;

  copy casebook_tables_and_columns_sorted 
  to '/tmp/casebook_tables_and_columns_new.csv' 
  --to 'casebook_tables_and_columns_new.csv'
  WITH DELIMITER AS ','
  CSV force quote description;
  
  copy casebook_tables_sorted 
  to '/tmp/casebook_tables_new.csv' 
  --to 'casebook_tables_new.csv'
  WITH DELIMITER AS ','
  CSV force quote description;
   
end
$$ LANGUAGE plpgsql;
