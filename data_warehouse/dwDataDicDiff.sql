DROP TABLE IF EXISTS dw_tables_and_columns;
DROP TABLE IF EXISTS dw_tables;

CREATE TABLE dw_tables_and_columns(
  tablename varchar(255) not null,
  columnname varchar(255) not null,
  datatype varchar(50) not null,
  nbytes_fixed integer default null,
  nbytes_variable integer default null,
  description text default null,
  foreign_key_links_to varchar(255) default null,
  cb2_source_table_column varchar(255) default null,
  population_logic text default null,
  change_type varchar(20) default null,
  change_detected_on date default null,
  primary key (tablename, columnname)
);

CREATE TABLE dw_tables(
  tablename varchar(255) not null,
  description text default null,
  fact_or_dimension text default null,
  change_type varchar(20) default null,
  change_detected_on date default null,
  primary key (tablename)
);

COPY dw_tables_and_columns FROM '/tmp/dw_tables_and_columns.csv' WITH CSV HEADER;
COPY dw_tables FROM '/tmp/dw_tables.csv' WITH CSV HEADER;

update dw_tables_and_columns
  set description = '"' || description || '"',
      population_logic = '"' || population_logic || '"';
update dw_tables 
  set description = '"' || description || '"',
      fact_or_dimension = '"' || fact_or_dimension || '"';


DROP FUNCTION IF EXISTS dwDataDicDiff(); 

CREATE FUNCTION dwDataDicDiff() RETURNS void AS $$
  
  DECLARE

    dataDicRow dw_tables_and_columns%ROWTYPE;

    tableName pg_class.relname%TYPE;
    columnName pg_attribute.attname%TYPE;
    dataType pg_type.typname%TYPE;
    fixedLength pg_attribute.attlen%TYPE;
    variableLength pg_attribute.atttypmod%TYPE;
    recordFromMetadata RECORD;
    recordFromDataDictionary RECORD;
    nMatchingRecords integer;

    --CURSOR THAT FETCHES DATA FROM METADATA. USED TO SEE WHAT TABLES AND COLUMNS HAVE BEEN ADDED.
    
    cursForAdded CURSOR FOR 
      SELECT c.relname AS tableName, a.attname AS columnName, t.typname AS dataType, a.attlen AS fixedLength,
        a.atttypmod AS variableLength
      FROM pg_class c, pg_attribute a, pg_type t
      WHERE a.attnum > 0
      AND a.attrelid = c.oid
      AND a.atttypid = t.oid
      and c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' 
      and a.attname != 'created_at' and a.attname != 'updated_at' and a.attname != 'id'
      and c.relname not in ('dw_tables_and_columns', 'dw_tables', 'jobs', 'tasks', 'runs', 'schema_migrations')
      order by c.relname, a.attname;

    --CURSOR THAT FETCHES DATA FROM DATA DICTIONARY. USED TO SEE WHAT TABLES AND COLUMNS HAVE BEEN DROPPED.

    cursForDropped CURSOR FOR SELECT * FROM dw_tables_and_columns;

    --CURSOR THAT FETCHES DATA FROM METADATA. USED TO SEE WHAT TABLES HAVE BEEN ADDED.
    
    cursForAddedTable CURSOR FOR 
      SELECT c.relname AS tableName
      FROM pg_class c
      WHERE c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' 
      and c.relname NOT IN ('dw_tables_and_columns', 'dw_tables', 'jobs', 'tasks', 'runs', 'schema_migrations')
      order by c.relname;

    --CURSOR THAT FETCHES DATA FROM DATA DICTIONARY. USED TO SEE WHAT TABLES HAVE BEEN DROPPED.

    cursForDroppedTable CURSOR FOR SELECT * FROM dw_tables;

  BEGIN
    
    
    FOR recordFromMetadata IN cursForAdded LOOP
      
      --CHECK IF THESE TABLES/COLUMNS ARE PRESENT IN THE DATA DICTIONARY
      
      EXECUTE 'select count(*)  from dw_tables_and_columns where tablename = $1 and columnname = $2' 
              INTO nMatchingRecords USING recordFromMetadata.tableName, recordFromMetadata.columnName;
      IF nMatchingRecords = 0 THEN
          RAISE NOTICE 'column % in table % is a new addition', recordFromMetadata.columnName, 
                       recordFromMetadata.tableName;
          INSERT INTO dw_tables_and_columns(tablename, columnname, datatype, nbytes_fixed, nbytes_variable, change_type, change_detected_on) 
                     VALUES (recordFromMetadata.tableName, recordFromMetadata.columnName, recordFromMetadata.dataType, 
                        NULLIF(recordFromMetadata.fixedLength, -1), NULLIF(recordFromMetadata.variableLength, -1), 'added', 
                        current_date);
        --END IF;
      END IF;
     
    END LOOP;

    FOR recordFromDataDictionary IN cursForDropped LOOP
      
      --CHECK IF THESE TABLES/COLUMNS ARE PRESENT IN THE DATABASE
      
      EXECUTE 'select count(*) from pg_class c, pg_attribute a where a.attnum > 0 AND a.attrelid = c.oid 
               and c.relname !~ $1 AND c.relkind = $2 and c.relname = $3 and a.attname = $4'
         INTO nMatchingRecords 
         USING '^(pg_|sql_)', 'r', recordFromDataDictionary.tablename, 
               recordFromDataDictionary.columnname; 
      IF nMatchingRecords = 0 THEN
         --RAISE NOTICE 'column % in table % has been dropped', recordFromDataDictionary.columnname, recordFromDataDictionary.tablename;
         UPDATE dw_tables_and_columns SET change_type = 'dropped', change_detected_on = current_date 
         WHERE dw_tables_and_columns.tablename = recordFromDataDictionary.tablename 
         and dw_tables_and_columns.columnname = recordFromDataDictionary.columnname;
      END IF;
     
    END LOOP;

    FOR recordFromMetadata IN cursForAddedTable LOOP
      
      --CHECK IF THESE TABLES ARE PRESENT IN THE DATA DICTIONARY
      
      EXECUTE 'select count(*)  from dw_tables where tablename = $1' INTO nMatchingRecords USING recordFromMetadata.tableName;
      IF nMatchingRecords = 0 THEN
         --RAISE NOTICE 'table % is a new addition', recordFromMetadata.tableName;
         INSERT INTO dw_tables(tablename, change_type, change_detected_on) 
                     VALUES (recordFromMetadata.tableName, 'added', current_date);
      END IF;
     
    END LOOP;

    FOR recordFromDataDictionary IN cursForDroppedTable LOOP
      
      --CHECK IF THESE TABLES ARE PRESENT IN THE DATABASE
      
      EXECUTE 'select count(*) from pg_class c where c.relname = $1'
         INTO nMatchingRecords 
         USING recordFromDataDictionary.tablename;
      IF nMatchingRecords = 0 THEN
         --RAISE NOTICE 'table % has been dropped', recordFromDataDictionary.tablename;
         UPDATE dw_tables SET change_type = 'dropped', change_detected_on = current_date 
         WHERE dw_tables.tablename = recordFromDataDictionary.tablename;
      END IF;
    END LOOP;
  END;
$$ LANGUAGE plpgsql;

--Run the function to populate and update dw_tables_and_columns
SELECT dwDataDicDiff();

--Copy table and column data back to csv files. Before that, copy to a new 
--table to sort the data.
DROP TABLE IF EXISTS dw_tables_and_columns_new;
DROP TABLE IF EXISTS dw_tables_new;

CREATE TABLE dw_tables_and_columns_new(
  tablename varchar(255) not null,
  columnname varchar(255) not null,
  datatype varchar(50) not null,
  nbytes_fixed integer default null,
  nbytes_variable integer default null,
  description text default null,
  foreign_key_links_to varchar(255) default null,
  cb2_source_table_column varchar(255) default null,
  population_logic text default null,
  change_type varchar(20) default null,
  change_detected_on date default null,
  primary key (tablename, columnname)
);

CREATE TABLE dw_tables_new(
  tablename varchar(255) not null,
  description text default null,
  fact_or_dimension text default null,
  change_type varchar(20) default null,
  change_detected_on date default null,
  primary key (tablename)
);

INSERT INTO dw_tables_and_columns_new SELECT * FROM dw_tables_and_columns ORDER BY tablename, columnname;
INSERT INTO dw_tables_new SELECT * FROM dw_tables ORDER BY tablename;
COPY dw_tables_and_columns_new TO '/tmp/dw_tables_and_columns_new.csv' delimiters ',';
COPY dw_tables_new TO '/tmp/dw_tables_new.csv' delimiters ',';
--DROP TABLE dw_tables_and_columns;
--DROP TABLE dw_tables_and_columns_new;
--DROP TABLE dw_tables;
--DROP TABLE dw_tables_new;
