# Copyright 2017 Snowflake Computing Inc.
# (derived from dplyr, Copyright 2013-2015 RStudio)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @import DBI
#' @import RJDBC
#' @import dbplyr
#' @import dplyr
#' @import glue
#' @import futile.logger
NULL

#' dplyr backend support for SnowflakeDB (https://snowflake.net)
#'
#' Use \code{src_snowflakedb} to connect to an existing Snowflake database,
#' and \code{tbl} to connect to tables within that database.
#'

NOT_NULLS <- c('\\N', 'NULL', 'NUL', '<NULL>', '<Null>', '', '.', 'NA')

setClass(
  "SnowflakeDBConnection",
  representation = representation(
    con = "JDBCConnection",
    jc = "jobjRef",
    identifier.quote = "character",
    info = "list"
  ),
  contains = "JDBCConnection"
)

.SnowflakeDBConnection <- function(con, info) {
  new(
    "SnowflakeDBConnection",
    con = con,
    jc = con@jc,
    identifier.quote = con@identifier.quote,
    info = info
  )
}

#' @template db-info
#' @param user Username
#' @param password Password
#' @param account Account Name (e.g. <account>.snowflakecomputing.com)
#' @param host Hostname (Not required for public endpoints, defaults to
#'             <account>.snowflakecomputing.com)
#' @param port Port (Defaults to 443, the default for public endpoints)
#' @param opts List of other parameters to pass (warehouse, db, schema, tracing)
#' @param region_id Specifies the ID for the Snowflake Region where your account 
#' is located. (Default: us-west, example: us-east-1). 
#' See: \url{https://docs.snowflake.net/manuals/user-guide/intro-editions.html#region-ids-in-account-urls}
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}. For the tbl, included for
#'   compatibility with the generic, but otherwise ignored.
#' @param src a snowflakedb src created with \code{src_snowflakedb}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' my_db <- src_snowflakedb(user = "snowman",
#'                          password = "letitsnow",
#'                          account = "acme",
#'                          opts = list(warehouse = "mywh",
#'                                      db = "mydb",
#'                                      schema = "public")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' \donttest{
#' # Here we'll use the Lahman database: to create your own in-database copy,
#' # create a database called "lahman", or tell lahman_snowflakedb() how to
#' # connect to a database that you can write to
#'
#' #if (has_lahman("snowflakedb", account = "acme",
#' #               user = "snowman", password = "letitsnow",
#' #               opts=list(warehouse="wh", db="lahman", schema="public"))) {
#' lahman_p <- lahman_snowflakedb()
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_p, "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = if(is.null(AB)) 1.0 * R / AB else 0)
#'
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#'
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#'
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#' best_year <- filter(players, AB == max(AB) | G == max(G))
#' progress <- mutate(players,
#'   cyear = yearID - min(yearID) + 1,
#'   ab_rank = rank(desc(AB)),
#'   cumulative_ab = order_by(yearID, cumsum(AB)))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(stints, stints > 3)
#' summarise(stints, max(stints))
#' # mutate(stints, order_by(yearID, cumsum(stints)))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_p, "Master"), playerID, birthYear)
#' hof <- select(filter(tbl(lahman_p, "HallOfFame"), inducted == "Y"),
#'  playerID, votedBy, category)
#'
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # Find players not in hof
#' anti_join(player_info, hof)
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_p,
#'   sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
#' batting2008
#' #}
#' }

src_snowflakedb <- function(user = NULL,
                            password = NULL,
                            account = NULL,
                            port = 443,
                            host = NULL,
                            opts = list(),
                            region_id = "us-west",
                            ...) {
  requireNamespace("RJDBC", quietly = TRUE)
  requireNamespace("dplyr", quietly = TRUE)
  
  valid_regions = c("us-east-1", "eu-central-1", "ap-southeast-2")
  
  isWest <- substring(tolower(region_id), 1, 7) == "us-west"
  
  if (!isWest && !(tolower(region_id) %in% valid_regions)) {
    stop("Invalid 'region_id'. 
         See: https://docs.snowflake.net/manuals/user-guide/intro-editions.html#region-ids-in-account-urls")
  }
  
  # set client metadata info
  snowflakeClientInfo <- paste0(
    '{',
    '"APPLICATION": "dplyr.snowflakedb",',
    '"dplyr.snowflakedb.version": "',
    packageVersion("dplyr.snowflakedb"),
    '",',
    '"dplyr.version": "',
    packageVersion("dplyr"),
    '",',
    '"R.version": "',
    R.Version()$version.string,
    '",',
    '"R.platform": "',
    R.Version()$platform,
    '"',
    '}'
  )
  
  # initalize the JVM and set the snowflake properties
  .jinit()
  .jcall(
    "java/lang/System",
    "S",
    "setProperty",
    "snowflake.client.info",
    snowflakeClientInfo
  )
  
  if (length(names(opts)) > 0) {
    opts <- paste0("&",
                   paste(lapply(names(opts),
                                function(x) {
                                  paste(x, opts[x], sep = "=")
                                }),
                         collapse = "&"))
  } else {
    opts <- ""
  }
  
  message("host: ", host)
  
  if (is.null(host) || host == "") {
    regionUrl <- if (isWest) "" else paste0(".", tolower(region_id))
    
    host = paste0(account, regionUrl, ".snowflakecomputing.com")
  }
  
  url <- paste0("jdbc:snowflake://",
                host,
                ":",
                as.character(port),
                "/?account=",
                account,
                opts)
  message("URL: ", url)
  conn <-
    dbConnect(
      RJDBC::JDBC(
        driverClass = "com.snowflake.client.jdbc.SnowflakeDriver",
        classPath = getOption('dplyr.jdbc.classpath', NULL),
        identifier.quote = "\""
      ),
      url,
      user,
      password,
      ...
    )
  res <- get_query(
    conn,
    'SELECT
    CURRENT_USER() AS USER,
    CURRENT_DATABASE() AS DBNAME,
    CURRENT_VERSION() AS VERSION,
    CURRENT_SESSION() AS SESSIONID'
  )
  info <- list(
    dbname = res$DBNAME,
    url = url,
    version = res$VERSION,
    user = res$USER,
    Id = res$SESSIONID
  )

  env <- environment()
  # temporarily suppress the warning messages from getPackageName()
  wmsg <- getOption('warn')
  options(warn = -1)
  SnowflakeDBConnection <-
    methods::setRefClass(
      "SnowflakeDBConnection",
      fields = c("con", "jc", "identifier.quote", "info"),
      contains = c("JDBCConnection"),
      where = env
    )
  options(warn = wmsg)
  
  con <- structure(conn, info = info, class = c("SnowflakeDBConnection", "JDBCConnection"))
   
  # Creates an environment that disconnects the database when it's
  # garbage collected
  db_disconnector <- function(con, name, quiet = FALSE) {
    reg.finalizer(environment(), function(...) {
      if (!quiet) {
        message(
          "Auto-disconnecting ",
          name
        )
      }
      dbDisconnect(con)
    })
    environment()
  }
  
  dbplyr::src_sql("snowflakedb",
                  con,
                  disco = db_disconnector(con, "snowflakedb"))
}

#' @export
#' @rdname src_snowflakedb
tbl.src_snowflakedb <- function(src, from, ...) {
  dbplyr::tbl_sql("snowflakedb", src = src, from = from, ...)
}

#' @export
tbl.SnowflakeDBConnection <- function(con, from, ...) {
  src <- dbplyr::src_sql("snowflakedb", con)
  dbplyr::tbl_sql("snowflakedb", src = src, from = from, ...)
}

#' @export
db_desc.SnowflakeDBConnection <- function(x) {
  info <- x@info
  # Commented-out so that regression tests don't have to be updated with every new release. 
  # Instead we use a version and URL-agnostic printout. 
  
  # paste0("Snowflake Database: ", info$version, "\nURL: ", info$url, "\n")
  
  "SnowflakeDB Data Source"
}

#' @export
#sql_translate_env.src_snowflakedb <- function(x) {
sql_translate_env.SnowflakeDBConnection <- function(x) {
  dbplyr::sql_variant(
    dbplyr::base_scalar,
    dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function()
        dbplyr::sql("COUNT(*)"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd =  dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      # all = dbplyr::sql_prefix("bool_and"),
      # any = dbplyr::sql_prefix("bool_or"),
      paste = function(x, collapse)
        dbplyr::build_sql("LISTAGG(", x, collapse, ")")
    ),
    dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      n = function()
        dbplyr::sql("COUNT(*)"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd =  dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      # all = dbplyr::sql_prefix("bool_and"),
      # any = dbplyr::sql_prefix("bool_or"),
      paste = function(x, collapse)
        dbplyr::build_sql("LISTAGG(", x, collapse, ")")
    )
  )
}

# DBI methods ------------------------------------------------------------------
#' @export
db_has_table.SnowflakeDBConnection <- function(con, table) {
  table_qualified <- utils$dbplyr_schemify(table)
  table_name <- attr(table_qualified, 'table')
  schema_qualified <- attr(table_qualified, 'schema_qualified')
  flog.debug(glue('Checking if table {table_name} exists...'))
  dbExistsTable(con, table_name, schema_qualified) ||
    dbExistsTable(con, toupper(table), toupper(schema_qualified))
}

#' @export
db_analyze.SnowflakeDBConnection <- function(con, table) {
  # SnowflakeDB has no ANALYZE command so just return TRUE if called
  return(TRUE)
}

#' @export
db_create_index.SnowflakeDBConnection <-
  function(con, table, columns, name = NULL, ...) {
    # SnowflakeDB has no CREATE INDEX command so just return TRUE if called
    return(TRUE)
  }

#' @export
db_begin.SnowflakeDBConnection <- function(con, ...) {
  dbSendQuery(con, "BEGIN TRANSACTION")
}

#' @export
db_query_fields.SnowflakeDBConnection <- function(con, sql, ...) {
  fields <-
    dbplyr::build_sql("SELECT * FROM ", sql_subquery(con, sql), " LIMIT 0", con = con)
  if (isTRUE(getOption("dplyr.show_sql"))) message("SQL: ", sql)
  names(get_query(con, fields))
}

#' @export
db_data_type.SnowflakeDBConnection <- function(con, df) {
  data_type <- function(x) {
    switch(
      class(x)[1],
      logical = "BOOLEAN",
      integer = "NUMBER",
      numeric = "DOUBLE",
      factor =  "VARCHAR",
      character = "VARCHAR",
      Date = "DATE",
      POSIXct = "TIMESTAMP",
      stop(
        "Can't map type ",
        paste(class(x), collapse = "/"),
        " to a supported database type."
      )
    )
  }
  types <- vapply(df, data_type, character(1))
  # Add NOT NULLs where no NAs:
  for (i in 1:length(types)) {
    if (any(is.na(df[[i]])) ||
      is.character(df[[i]]) && any(df[[i]] %in% NOT_NULLS)) next()
    types[[i]] <- glue::glue('{types[[i]]} NOT NULL')
  }
  types
}

#' @export
db_explain.SnowflakeDBConnection <- function(con, sql, ...) {
  message("explain() not supported for SnowflakeDB")
}


db_create_table.SnowflakeDBConnection <- function (
  con, table, types, temporary=TRUE, primary_key=NULL, replace=FALSE, do_upcase=TRUE
) {
  futile.logger::flog.info(glue('primary_key="{primary_key}"'))
  assert_that(
    is.character(table),
    length(table) == 1,
    is.character(types)
  )

  table <- utils$dbplyr_schemify(table)
  if (do_upcase) {
    names(types) <- toupper(names(types))
    primary_key <- toupper(primary_key)
  }
  col_names <- escape(
    ident(names(types)),
    collapse = NULL,
    con = con
  )
  pk_sql <- NULL
  # pk_sql <- if (is.character(primary_key) && nchar(primary_key) > 0) {
  #   paste('primary key', sql_vector(primary_key, collapse = ', ', con = con))
  # }
  col_specs <- sql_vector(
    c(
      paste(col_names, types),
      pk_sql
    ),
    parens = TRUE,
    collapse = ',\n',
    con = con
  )
  query <- build_sql(
    'create ',
    if (replace) sql('or replace '),
    if (temporary) sql('temporary '),
    'table ', as.sql(table), ' ',
    if (replace) sql('copy grants '),
    col_specs,
    con = con
  )

  dbExecute(con, query)
}

# Various helper methods ------------------------------------------------------------------

db_add_columns_to_table_definition <- function (src, df, table_name) {
  # Get table definition
  t <- src$get_tbl(table_name)
  # Get missing colnames
  missing_colnames <- setdiff(colnames(df), colnames(t))
  if (!length(missing_colnames)) return(NULL)
  # Get dtypes
  types <- db_data_type(src$con, df)
  # Add
  for (missing_colname in missing_colnames) {
    src$get_query(glue::glue(
      "alter table {table_name}
      add column {missing_colname} {types[[missing_colname]]}"
    ))
  }
}

db_drop_table_wrapper <- function (con, name, ...) {
  flog.debug('Dropping table {name}...')
  dplyr::db_drop_table(con, name, ...)
}

#' Perform a COPY INTO in Snowflake to perform a load or unload operation.
#'
#' Note that default opts here are different from Snowflake's default opts.
#'
#' This is to:
#' 1. align with tidyverse conventions, and
#' 2. make this amenable to R-produced files.
#'
#' Note that if TYPE is not provided,
#' then if utils$guess_delim(from) returns a delim,
#'   TYPE is set to 'CSV'. 
#'
#' Details:
#'
#' FIELD_DELIMITER defaults for
#' Snowflake: ','
#' db_snowflake_copy(): utils$guess_delim(from)
#'
#' NULL_IF defaults for
#' Snowflake: c('\\N')
#' db_snowflake_copy(): c('\\N', 'NULL', 'NUL', '<NULL>', '<Null>', '', '.', 'NA')
#' More vals are considered NULL per R convention.
#' NOTE WELL: NA is cast to NULL because there is no standard way to indicate NA in Snowflake.
#'
#' SKIP_HEADER defaults for
#' Snowflake: 0
#' db_snowflake_copy(): 1 if format_opts$TYPE='CSV', else do not override Snowflake's default.
#'
#' TRIM_SPACE defaults for
#' Snowflake: FALSE
#' db_snowflake_copy(): TRUE
#' This follows tidyverse convention.
#'
#' @param con A SnowflakeDBConnection object.
#' @param from The source of the data, i.e., what comes after the FROM in the COPY
#' statement. This can be a table, a subquery, a stage, or a local file.
#' @param to The target of the COPY statement as a string. This can be a S3/Azure/local
#' filesystem location, a table, or a Snowflake stage.
#' @param format_opts A list of key-value pairs for the Snowflake COPY file_format options.
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' my_db <- src_snowflakedb(user = "snowman",
#'                          password = "letitsnow",
#'                          account = "acme",
#'                          opts = list(warehouse = "mywh",
#'                                      db = "mydb",
#'                                      schema = "public")
#' # Copy from a local CSV file to a target table in my_db.
#' db_snowflake_copy(my_db$con, from = "file:///my/directory/foo.csv", to = "target_table", 
#' format_opts = list(format = 'csv', field_delimiter = ','))
#' }
#' # Copy from stage
#' db_snowflake_copy(
#'   my_db$con,
#'   from = '@warehouse.stages.my_s3_bucket_name/and/heres/data.tsv',
#'   to = 'WAREHOUSE.PUBLIC.FOO'
#' )
#' @export
db_snowflake_copy <- function(con, from, to, format_opts=list(), opts=list(), max_percent_error=0, with_metadata=FALSE) {
  to <- utils$dbplyr_schemify(id(to))
  format_opts <- local({
    not_nulls_quoted <- sapply(
      sapply(
        NOT_NULLS,
        function (item) stringr::str_replace(item, '\\\\', '\\\\\\\\')[[1]]
      ),
      function (item) glue::glue("'{item}'")
    )
    general_default_opts <- list(
      NULL_IF = glue::glue("({paste(not_nulls_quoted, collapse = ', ')})"), # NOTE WELL: `\\N` (`\\\\N` when escaped) assumes the ESCAPE_UNENCLOSED_FIELD value is \\ (that opt's default).
      TRIM_SPACE = TRUE
    )
    if (!length(format_opts$TYPE)) {
      # 'CSV' is Snowflake's default TYPE.
      #' Set it here to make the logic below easier.
      format_opts$TYPE <- '"CSV"'
    }
    if (format_opts$TYPE != '"CSV"') {
      modifyList(
        general_default_opts,
        format_opts
      )
    } else {
      guessed_delim <- utils$guess_delim(from)
      default_field_delimiter <- if (is.na(guessed_delim)) {
        ',' # ',' is Snowflake's default FIELD_DELIMITER.
      } else {
        if (guessed_delim == '\t') '\\t' else guessed_delim
      }
      modifyList(
        general_default_opts,
        modifyList(
          list( # TYPE='CSV' default opts
            FIELD_DELIMITER = glue::glue('"{default_field_delimiter}"'),
            SKIP_HEADER = 1
          ),
          format_opts
        )
      )
    }
  })
  format_opts_str <- if (length(names(format_opts)) > 0) {
    paste0("file_format = (",
                   paste(lapply(names(format_opts),
                                function(x) {
                                  paste(x, format_opts[x], sep = "=")
                                }),
                         collapse = ", "), ")")
  } else {
    ""
  }
  # Get non-format opts ready:
  if (max_percent_error > 0) {
    opts <- modifyList(
      opts,
      list(on_error = 'CONTINUE')
    )
  }
  base_opts_str <- paste(
    lapply(
      names(opts),
      function (opt) glue::glue("{opt}='{opts[opt]}'")
    ),
    collapse = '\n'
  )
  opts_str <- paste(
    base_opts_str,
    format_opts_str,
    collapse = '\n'
  )
  
  get_query(con, glue::glue("begin transaction name copy_into"))
  if (with_metadata) {
    # ENHANCE: How `to_colnames` is got.
    to_colnames <- colnames(get_query(
      con,
      glue::glue(
        "select *
        from {to}
        limit 0"
      )
    ))
    col_idx_exprs <- sapply(
      1:(length(to_colnames) - 2), # - 2 for FILENAME, FILE_ROW_NUMBER
      function (col_idx) glue::glue("${col_idx}")
    )
    # ENHANCE: Support FILENAME and FILE_ROW_NUMBER not being last cols
    res <- get_query(
      con,
      glue::glue(
        "copy into {to}
        from (
          select {paste(col_idx_exprs, collapse = ', ')}, metadata$FILENAME, metadata$FILE_ROW_NUMBER
          from '{from}'
        )
        {opts_str}"
      )
    )
  } else {
    res <- get_query(con, paste("COPY INTO", to, "FROM", from, opts_str))
  }
  # res <- get_query(con, paste("COPY INTO", to, "FROM", from, opts_str))
  if (max_percent_error != 0 && any(res$errors_seen != 0)) {
    for (file_res_i in 1:nrow(res)) {
      file_res <- res[file_res_i, ]
      percent_error <- (1 - (file_res$rows_loaded / file_res$rows_parsed)) * 100
      if (percent_error > max_percent_error) {
        get_query(con, "rollback")
        flog.error(glue::glue('percent_error="{percent_error}" > max_percent_error="{max_percent_error}"'), capture = TRUE, res)
        stop(glue::glue('percent_error="{percent_error}" > max_percent_error="{max_percent_error}"'))
      }
    }
  }
  get_query(con, glue::glue("commit"))

  if (any(res$errors_seen != 0)) flog.warn('Error in copy:', capture = TRUE, res)
  res
}

#' Put a file(s) to a stage.
#'
#' @param file_path is a string of the local filepath(s) to be uploaded.
#'    Wildcards are accepted to PUT multiple files at once.
#'    Details can be read here: https://docs.snowflake.net/manuals/sql-reference/sql/put.html
#'
#' @param dest is a string of the stage + folder where files will be PUT.
#'    Ex.: '@warehouse.stages.foo_stage/foo/bar'.
#'    If the folder(s) do not already exist, they will be recursively created.
#'
#' @param opts is a name-value list of opts.
#'    Details can be read here: https://docs.snowflake.net/manuals/sql-reference/sql/put.html
#'    Of note, AUTO_COMPRESS=FALSE is one you might want to use.
#'
#' @export
db_put_to_stage <- function(con, file_path, dest, opts = list()) {
  file_path <- paste0("file://", file_path)
  opts_sql <- if (length(opts)) {
    paste(
      lapply(
        names(opts),
        function (name) glue::glue('{name}={opts[[name]]}')
      ),
      collapse = ', '
    )
  } else {
    ''
  }
  rs <- get_query(con, glue("PUT '{file_path}' {dest} {opts_sql}"))
  if (rs["status"] != "UPLOADED") warning(rs)
}

get_query <- function (con, query) {
  flog.debug(glue('Sending query {query}...'))
  dbGetQuery(con, query)
}

#' @export
db_get_from_stage <- function(con, file_name, stage_name) {
  get_query(con, paste0("GET ", stage_name, " '", file_name, "'"))
}

#' @export
db_create_stage <- function(con, stage_name, temporary = FALSE) {
  temp <- ifelse(temporary, "TEMP", "")
  dbSendQuery(con, paste("CREATE OR REPLACE", temp, "STAGE", stage_name))
}

#' Load file into database table
#'
#' If the table does not exist, an err is thrown.
#'
#' @param con A connection obj.
#'
#' @param table_name A string of the database table name being loaded.
#'
#' @param file_path A string of the file_path to be loaded.
#'
#' @param stage_dir A string of the stage destination, including folder.
#'    Eg, '@warehouse.stages.foo_stage/foo/bar'.
db_load_from_file <- function(con, table_name, file_path, stage_dir) {
  stopifnot(file.exists(file_path))
  table_name <- utils$dbplyr_schemify(table_name)
  
  if (!db_has_table(con, table_name)) stop("The specified table does not exist in Snowflake.")

  db_put_to_stage(con, file_path, dest = stage_dir)

  db_snowflake_copy(
    con,
    from = paste(stage_dir, basename(file_path), sep = '/'),
    to = table_name
  )
}

#' Atomically copy file into database table
#'
#' atomic_copy() accomplishes atomicity by creating a intermediate table,
#' loading into that intermediate table, then
#' if the final table already exists,
#'    swapping the intermediate with the final table (then dropping the old final table),
#' else
#'    renaming the intermediate to the final table name.
#'
#' Note that Snowflake automatically commits pending session transactions when
#' a DDL query is performed.
#'
#' @param con a connection obj.
#'
#' @param from a string of the filepath to copy.
#'
#' @param to a string of the table name to copy into.
#'
#' @param stage_name a string of the stage name (including folders) where file will be PUT before loading.
#'    Eg, this could be a location on AWS's S3.
#'
#' @param temporary a logical.
#'    If TRUE, then if a table is created, it is created as a temp table.
atomic_copy <- function(con, from, to, stage_name, temporary = FALSE) {
  to <- utils$dbplyr_schemify(to)
  intermediate_table_name <- paste0(to, "_dplyr_snowflakedb_intermediate_table")
  tryCatch({
    dbSendQuery(
      con,
      glue::glue("create or replace {if (temporary) 'temp' else ''} table {id(intermediate_table_name)} like {id(to)}")
    )
    db_load_from_file(
      con,
      table_name = intermediate_table_name,
      file_path = from,
      stage_dir = stage_name
    )
    
    if (db_has_table(con, to)) {
      dbSendQuery(con,
                  paste("ALTER TABLE", id(to), "SWAP WITH", id(intermediate_table_name)))
    } else {
      dbSendQuery(con,
                  paste("ALTER TABLE", id(intermediate_table_name), "RENAME TO", id(to)))
    }
  },
  # If PUT or COPY fail even partially, be sure to catch it and stop alteration of original table
  warning = function(w) {
    stop(w)
  },
  finally = {
   dbSendQuery(con,
               paste("DROP TABLE IF EXISTS", intermediate_table_name))
  })
}

# Override copy_into and db_insert_into to use bulk COPY ------------------------------------------

#' Copy a local data frame into a remote data source
#' 
#' copy_to() copies a local data frame into a remote data source,
#' creating the table definition as needed.
#'
#' @name copy_to
#'
#' @param dest Either a SnowflakeDBConnection
#'    or having a SnowflakeDBConnection at dest$con (eg, a src_snowflakedb instance).
#'
#' @param df a data.frame.
#'
#' @param name a string of the table name to be copied into.
#'    Defaults to the name of arg df:
#'    Eg, if you call copy_to(dest, df = MY_DF),
#'    then name will be 'MY_DF'.
#'
#' @param overwrite a logical.
#'    If TRUE, then target table will be overwritten if exists.
#'    Else, target table will not be overwritten if exists.
#'
#' @param mode a string of 'safe', 'overwrite', or 'append'.
#'    If 'safe' (the default), then no table change will occur if table already exists.
#'    If 'overwrite', then the table will be recreated.
#'    If 'append', then the table will be appened to if it already exists.
#'    If arg overwrite is TRUE, then mode will be set to 'overwrite'.
#'
#' @param stage_name a string of the stage name (including folders) where file will be PUT before loading.
#'    Eg, this could be a location on AWS's S3.
#'
#' @param temporary a logical indicating whether or not created tables are temporary.
#'
#' @param ... Passed to dbplyr::db_write_table() and atomic_copy() when they're called.
#'    Of note, `temporary` is a logical for whether or not the created table is temporary.
#'
#' @export
copy_to.src_snowflakedb <- function (
  dest,
  df,
  name = deparse(substitute(df)),
  overwrite = FALSE,
  mode = "safe",
  stage_name,
  temporary = FALSE,
  ...
) {
  mode <- if (overwrite) 'overwrite' else tolower(mode)

  connection <- if (utils$is_con(dest)) {
    dest
  } else if (utils$is_con(dest$con)) {
    dest$con
  } else {
    stop(glue::glue('copy_to() was provided arg dest without a src_SQLiteConnection or SnowflakeDBConnection.'))
  }
  connection_type <- utils$get_connection_type(connection)
  if (connection_type == 'snowflake') {
    # snowflake-specific validations:
    if (length(stringr::str_split(name, '\\.')[[1]]) != 3) stop(glue::glue('copy_to() requires a fully qualified table name.  Eg, "WAREHOUSE.PUBLIC.FOO_TABLE".  This is to prevent overwriting tables with the same name in a different schema.'))
  }
  stopifnot(is.data.frame(df),
            is.string(name),
            is.string(stage_name),
            mode %in% c("safe", "overwrite", "append"))
  
  df <- local({
    for (colname in colnames(df)) {
      if ('character' %in% class(df[[colname]])) {
        df[[colname]] <- trimws(df[[colname]])
      }
    }
    df
  })
  name <- utils$dbplyr_schemify(name)
  
  has_table <- db_has_table(connection, name)
  
  if (has_table) {
    message("The table ", name, " already exists.")
    if (mode != "overwrite" && mode != "append") {
      stop(
        "Could not copy because the table already exists in the database.
        Set parameter 'mode' to either \"overwrite\"or \"append\"."
      )
    }
  }
  

  types <- db_data_type(connection, df)
  names(types) <- names(df)
  
  tryCatch({
    tmpfilename = tempfile(fileext = ".tsv")
    flog.debug(glue('Writing to tempfile {tmpfilename}...'))
    readr::write_delim(
      df,
      tmpfilename,
      "\t"
    )
    if (mode == 'overwrite' && has_table) {
      db_drop_table_wrapper(connection, name)
    }
    if (has_table && mode != 'overwrite') {
      db_load_from_file(
        connection,
        name,
        file_path = tmpfilename,
        stage_dir = stage_name
      )
    } else {
      db_create_table(connection, utils$dbplyr_schemify(name), types, temporary = temporary, ...)
      
      atomic_copy(
        con = connection,
        from = tmpfilename,
        to = name,
        stage_name = stage_name,
        temporary = temporary
      )
    }
  },
  error = function(e) {
    if (!has_table && db_has_table(connection, name)) {
      db_drop_table_wrapper(connection, name)
    }
    
    stop(e)
  })
  
  tbl(connection, name)
}

#' Copies each staged file into table.
#'
#' If table does not exist,
#'    then creates table, downloading a file to determine data types.
#'
#' Depends on S3 utils.
#'
#' @name copy_to_from_staged_files
#'
#' @param con A connection obj.
#'
#' @param table_name a string of the table name to be copied into.
#'
#' @param stage_with_prefix a string of the stage + files' prefix.
#'
#' @param mode a string of 'safe', 'overwrite', or 'append'.
#'    If 'safe' (the default), then no table change will occur if table already exists.
#'    If 'overwrite', then the table will be recreated.
#'    If 'append', then the table will be appened to if it already exists.
#'    If arg overwrite is TRUE, then mode will be set to 'overwrite'.
#'
#' @param temporary a logical indicating whether or not created tables are temporary.
#'
#' @param delim a string indicating the files' delim.  Guessed if not provided.
#'
#' @param df a data.frame to be used to create table (if applicable).  Defaults to NULL.
copy_to_from_staged_files <- function (con, table_name, stage_with_prefix, mode, FILES_METADATA, temporary=FALSE, delim=NULL, df) {
  stopifnot(
    is.character(table_name),
    is.character(stage_with_prefix),
    mode %in% c("safe", "overwrite", "append")
  )
  table_name <- utils$dbplyr_schemify(table_name)
  does_db_have_table <- db_has_table(con, table_name)
  if (mode == 'safe' && does_db_have_table) stop("The specified table already exists in Snowflake.")
  if (mode == 'overwrite') {
    # create or replace table
    copy_to(
      con,
      df,
      name = table_name,
      overwrite = TRUE,
      mode = mode,
      stage_name = stringr::str_split(stage_with_prefix, '/')[[1]][[1]],
      temporary = temporary
    )
  }
  for (i in 1:nrow(FILES_METADATA)) {
    FILE_METADATA <- FILES_METADATA[1, ]
    db_snowflake_copy(
      con,
      from = stage_with_prefix,
      to = table_name,
      opts = list(pattern = '.*')
    )
  }
}



#' @export
copy_to.SnowflakeDBConnection <- copy_to.src_snowflakedb

#' @export
db_insert_into.SnowflakeDBConnection <-
  function(con, table, values, ...) {
    copy_to(
      dest = con,
      df = values,
      name = table,
      mode = "append",
      ...
    )
  }

id <- function(name) {
  names <- stringr::str_split(name, '\\.')
  paste(
    sapply(names, function (name) dbplyr::sql_quote(name, '"')),
    collapse = '.'
  )
}
# snowflake.identifier <- function(name) {
#   if (is.quoted(name)) 
#     name
#   else
#     paste0("\"", toupper(name), "\"")
# }
# 
# is.quoted <- function(string) {
#   # In R-3.4, can be replaced with base::startsWith and base::endsWith
#   #startsWith(string, "\"") && endsWith(string, "\"")
#    
#   substring(string, 1, 1) == "\"" && 
#     substring(string, nchar(string), nchar(string)) == "\""
# }

#' @export
setMethod("dbGetRowsAffected", signature("JDBCResult"), function(res) {
  0
})

#' #' @export
#' db_insert_into.SnowflakeDBConnection <-
#'   function(con, table, values, ...) {
#'     # write the table out to a local tsv file
#'     tmp <- tempfile(fileext = ".tsv")
#'     write.table(
#'       values,
#'       tmp,
#'       sep = "\t",
#'       quote = FALSE,
#'       row.names = FALSE,
#'       col.names = TRUE
#'     )
#'
#'     # put the tsv file to the Snowflake table stage
#'     message(sql)
#'     rs <- get_query(con, sql)
#'     if (rs["status"] != "UPLOADED")
#'       print(rs)
#'
#'     # load the file from the table stage
#'     sql <-
#'       dbplyr::build_sql(
#'         "COPY INTO ",
#'         ident(table),
#'         " FILE_FORMAT = (FIELD_DELIMITER = '\\t' SKIP_HEADER = 1 NULL_IF = 'NA')"
#'       )
#'     message(sql)
#'     rs <- get_query(con, sql)
#'     if (rs["errors_seen"] != "0")
#'       print(rs)
#'
#'   }
