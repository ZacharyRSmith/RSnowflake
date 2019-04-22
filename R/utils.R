dbplyr_schemify <- function (name) {
  if ('ident' %in% class(name)) return(name) # Has already been schemified.
  parts <- stringr::str_split(name, '\\.')[[1]]
  if (length(parts) == 1) {
    table <- name
  } else {
    database <- if (length(parts) == 2) NULL else parts[[1]]
    schema <- if (length(parts) == 2) parts[[1]] else parts[[2]]
    schema_qualified <- if (is.null(database)) {
      schema
    } else {
      paste(database, schema, sep = '.')
    }
    table <- parts[[length(parts)]]
    name <- dbplyr::in_schema(
      schema_qualified, 
      table
    )
    attr(name, 'database') <- database
    attr(name, 'schema_qualified') <- schema_qualified
    attr(name, 'schema') <- schema
  }
  attr(name, 'table') <- table
  attr(name, 'table_qualified') <- if (exists('schema_qualified')) {
    paste(schema_qualified, table, sep = '.')
  } else {
    table
  }
  name
}

get_connection_type <- function (con) {
  # Suppressing warnings like:
  # Warning message:
  # In if (grepl("src_SQLiteConnection", class(connection))) { :
  #   the condition has length > 1 and only the first element will be used
  suppressWarnings({
    if (grepl('src_SQLiteConnection', class(con))) {
      'sqlite'
    } else {
      'snowflake'
    }
  })
}

#' guess_delim() returns the delim for filepath.
#'
#' @param filepath a string of a filepath.
#' @return a string of filepath's delim.
#' @examples
#'
#' guess_delim('foobar.csv.gz')
#' --> ','
guess_delim <- function (filepath) {
  # Don't match to end of string so we can handle, eg, 'foo.csv.gz'
  if (grepl('\\.psv\\.|\\.psv$', filepath)) {
    '|'
  } else if (grepl('\\.tsv\\.|\\.tsv$', filepath)) {
    '\t'
  } else if (grepl('\\.csv\\.|\\.csv$', filepath)) {
    ','
  } else {
    NA
    # stop(glue::glue('Could not guess delim from filepath="{filepath}"'))
  }
}

is_con <- function (obj) {
  # Suppressing warnings like:
  # Warning message:
  # In if (grepl("src_SQLiteConnection", class(connection))) { :
  #   the condition has length > 1 and only the first element will be used
  suppressWarnings({
    any(grepl(
      'src_SQLiteConnection|SnowflakeDBConnection',
      class(obj)
    ))
  })
}

#' Make sql exprs of from's col idxs mapped to to's cols.
#'
#' make_sql_col_idx_exprs() makes sql exprs of from's col idxs mapped to to's cols.
#' See examples.
#'
#' @param to_colnames a character vector of to's colnames, ordered.
#' @param from_colnames a character vector of from's colnames, ordered.
#' @examples
#' to_colnames <- c('X', 'Y')
#' from_colnames <- c('Y', 'X')
#'
#' actual <- make_sql_col_idx_exprs(
#'   to_colnames,
#'   from_colnames
#' )
#'
#' expect_equal(
#'   actual,
#'   c('$2', '$1')
#' )
make_sql_col_idx_exprs <- function (to_colnames, from_colnames, col_spec) {
  res <- sapply(
    from_colnames,
    function (colname) {
      col_idx <- which(to_colnames == colname)
      if (colname %in% col_spec$colname) {
        
        glue::glue("${col_idx}")
      } else {
        glue::glue("${col_idx}")
      }
    }
  )
  res <- sapply(
    from_colnames,
    function (colname) glue("${which(to_colnames == colname)}")
  )
  names(res) <- names(from_colnames)
  res
}

utils <- list(
  dbplyr_schemify = dbplyr_schemify,
  get_connection_type = get_connection_type,
  guess_delim = guess_delim,
  is_con = is_con,
  make_sql_col_idx_exprs = make_sql_col_idx_exprs
)
