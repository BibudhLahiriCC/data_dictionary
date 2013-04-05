SELECT --*
c.relname, a.attname, t.typname, nullif(a.attlen, -1) AS length,
	nullif(a.atttypmod, -1) AS lengthvar
FROM pg_class c, pg_attribute a, pg_type t
WHERE a.attnum > 0
AND a.attrelid = c.oid
AND a.atttypid = t.oid
and c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' 
and a.attname != 'created_at' and a.attname != 'updated_at' and a.attname != 'id'
and c.relname not in ('jobs', 'tasks', 'runs', 'schema_migrations')
--and a.attname like '%case%'
--and c.relname = 'child_locations'
order by c.relname, a.attname;

SELECT c.relname
FROM pg_class c
where c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' 
and c.relname not in ('jobs', 'tasks', 'runs', 'schema_migrations')
order by c.relname;

SELECT a.attname
FROM pg_class c, pg_attribute a
WHERE a.attnum > 0
AND a.attrelid = c.oid
and c.relname = 'reason_for_removals' 
and a.attname != 'id'
order by a.attname;

SELECT c.relname, count(a.attname)
FROM pg_class c, pg_attribute a
WHERE a.attnum > 0
AND a.attrelid = c.oid
and c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' 
and a.attname != 'created_at' and a.attname != 'updated_at' and a.attname != 'id'
and c.relname not in ('jobs', 'tasks', 'runs', 'schema_migrations')
--and a.attname like '%case%'
--and c.relname = 'child_locations'
group by c.relname
order by c.relname;

