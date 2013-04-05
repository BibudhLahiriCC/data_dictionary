library(gdata);

dwDataDicDiff <- function()
{
  library(RPostgreSQL);
  con <- dbConnect(PostgreSQL(), user="blahiri", password = "abc675", 
                   host = "localhost", port="5442", dbname="analytics");
  statement <- paste("SELECT c.relname, a.attname, t.typname, nullif(a.attlen, -1) AS length, ",
                     "nullif(a.atttypmod, -1) AS lengthvar ",
                     "FROM pg_class c, pg_attribute a, pg_type t ",
                     "WHERE a.attnum > 0 ",
                     "AND a.attrelid = c.oid ",
                     "AND a.atttypid = t.oid ",
                     "and c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' ",
                     "and a.attname != 'created_at' and a.attname != 'updated_at' and a.attname != 'id' ",
                     "and c.relname not in ('jobs', 'tasks', 'runs', 'schema_migrations') ",
                     "order by c.relname, a.attname", sep = "");
  res <- dbSendQuery(con, statement);
  tabs_cols_from_metadata <- fetch(res, n = -1);

  statement <- paste("SELECT c.relname AS tableName ",
                     "FROM pg_class c ",
                     "WHERE c.relname !~ '^(pg_|sql_)' AND c.relkind = 'r' ", 
                     "and c.relname NOT IN ('jobs', 'tasks', 'runs', 'schema_migrations') ",
                     "order by c.relname");
  res <- dbSendQuery(con, statement);
  tabs_from_metadata <- fetch(res, n = -1);

  tabs_from_dw <- read.csv("/Users/bibudhlahiri/data_dictionary/csv_files/dw_tables.csv", 
                           stringsAsFactors = FALSE);

  tabs_cols_from_dw <- read.csv("/Users/bibudhlahiri/data_dictionary/csv_files/dw_tables_and_columns.csv",
                                stringsAsFactors = FALSE);

  n_tabs_cols_from_metadata <- nrow(tabs_cols_from_metadata);
  row_idx_tabs_cols_from_dw <- nrow(tabs_cols_from_dw) + 1;

  for (i in 1:n_tabs_cols_from_metadata)
  {
    #Identify the newly added columns. Would be present in metadata but not in DW.
    matching_columns <- subset(tabs_cols_from_dw, ((trim(Table) == tabs_cols_from_metadata[i, "relname"]) & 
                               (trim(Attribute) == tabs_cols_from_metadata[i, "attname"])));
    if (nrow(matching_columns) == 0)
    {
      cat(paste("Column ", tabs_cols_from_metadata[i, "attname"], " in table ",
                tabs_cols_from_metadata[i, "relname"], " is a new addition\n", sep = ""));
      tabs_cols_from_dw[row_idx_tabs_cols_from_dw, ] <- c(tabs_cols_from_metadata[i, "relname"],
                                                          tabs_cols_from_metadata[i, "attname"],
                                                          tabs_cols_from_metadata[i, "typname"],
                                                          tabs_cols_from_metadata[i, "length"],
                                                          tabs_cols_from_metadata[i, "lengthvar"],
                                                          "", "", "", "", "added", 
                                                          format(Sys.Date(), format="%m/%d/%Y"));
     print(tabs_cols_from_dw[row_idx_tabs_cols_from_dw, ]);
     row_idx_tabs_cols_from_dw <- row_idx_tabs_cols_from_dw + 1;
    }
  }
  dbDisconnect(con);
  return(tabs_cols_from_dw);
}
