extend_src <- function (src) {
  snowflake_class_name = 'snowflake_class_name'
  # TODO: pass `...` to DBI funcs?
  src$get_query <- function (query, ...) {
    "
    Send @query to Snowflake, getting a data.frame of the results.
    
    @name Snowflake_get_query
    @param query A string of SQL
    @return data.frame
    "
    flog.info('running query: %s...', query, name = snowflake_class_name)
    DF <- DBI::dbGetQuery(src$con, query)
    if ('JDBCResult' %in% class(DF)) { # dbGetQuery() did not call dbFetch()/dbClearResult()
      DF <- DBI::dbFetch(res)
      DBI::dbClearResult(res)
    }
    flog.info('returning results from query: %s', query, name = snowflake_class_name)
    DF
  }
  src$get_tbl <- function (name, do_upcase=TRUE) {
    name <- toupper(name)
    if (length(stringr::str_split(name, '\\.')[[1]]) != 3) warning('Some functions do not work without a fully qualified table name (eg, WAREHOUSE.GEO.COUNTIES).')
    name <- utils$dbplyr_schemify(name)
    t <- dplyr::tbl(src, name)
    attr(t, 'table_names') <- list(
      database = attr(name, 'database'),
      schema = attr(name, 'schema'),
      table = attr(name, 'table')
    )
    t$columns <- function () {
      src$get_query(glue::glue(
        "select *
        from {attr(t, 'table_names')$database}.INFORMATION_SCHEMA.COLUMNS
        where TABLE_CATALOG = '{attr(t, 'table_names')$database}'
          and TABLE_SCHEMA = '{attr(t, 'table_names')$schema}'
          and TABLE_NAME = '{attr(t, 'table_names')$table}'
        order by ORDINAL_POSITION"
      ))
    }
    t$collect_and_cast <- function () {
      t_cols <- t$columns()
      RES <- t %>% dplyr::collect()
      for (colname in colnames(RES)) {
        data_type <- t_cols[
          t_cols$COLUMN_NAME == colname,
          'DATA_TYPE'
        ]
        caster <- if (grepl('NUM|DECIMAL|INT', data_type)) {
          as.integer
        } else if (grepl('FLOAT|DOUBLE|PRECISION|REAL', data_type)) {
          as.numeric
        } else if (grepl('BOOLEAN', data_type)) {
          as.logical
        } else if (grepl('CHAR|STRING|TEXT', data_type)) {
          as.character
        # Grepping for TIME must come before grepping for DATE,
        # because data_type could be DATETIME.
        } else if (grepl('TIME', data_type)) {
          lubridate::as_datetime
        } else if (grepl('DATE', data_type)) {
          lubridate::as_date
        } else {
          flog.info(glue::glue('Casting data_type="{data_type}" using {deparse(substitute(jsonlite::fromJSON))}'))
          jsonlite::fromJSON
        }
        RES[[colname]] <- caster(RES[[colname]])
      }
      RES
    }
    t
  }
  src
}
