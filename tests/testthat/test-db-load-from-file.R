suppressMessages({
  library(lubridate)
  library(testthat)
  devtools::load_all()
  options(dplyr.jdbc.classpath = Sys.getenv('SNOWFLAKE_JAR'))
})

context('db_load_from_file()')

`%||na%` <- function(a, b) if (is.null(a) || is.na(a)) b else a

cfg <- list(
  stage_name = Sys.getenv('RSNOWFLAKE_TESTS_STAGE'),
  src_type = Sys.getenv('RSNOWFLAKE_TESTS_SRC_TYPE', unset = NA) %||na% 'snowflakedb'
)
if (cfg$stage_name == '') stop('cfg$stage_name must be populated')

src <- if (cfg$src_type == 'sqlite') {
  src_sqlite(tempfile(), create = TRUE)
} else if (cfg$src_type == 'snowflakedb') {
  src_snowflakedb(account = Sys.getenv('SNOWFLAKE_ACCOUNT'),
                  host = Sys.getenv('SNOWFLAKE_HOST'),
                  user = Sys.getenv('SNOWFLAKE_USER'),
                  password = Sys.getenv('SNOWFLAKE_PASSWORD'),
                  opts = list(warehouse = Sys.getenv('SNOWFLAKE_WAREHOUSE'),
                              db = Sys.getenv('SNOWFLAKE_DB'),
                              role = Sys.getenv('SNOWFLAKE_ROLE'),
                              schema = 'public',
                              tracing = 'off'))
} else {
  stop(glue::glue('cfg$src_type="{cfg$src_type}" not supported'))
}

src <- extend_src(src)

test_that('@param col_names enables loading files with differently ordered cols', {
  skip_if(cfg$src_type == 'sqlite')
  table_name <- 'SANDBOX.PUBLIC.TEST_COL_NAMES'

  df1 <- data.frame(X = 'x1', Y = 'y1')
  t <- copy_to(
    dest = src,
    df = df1,
    name = table_name,
    temporary = TRUE,
    stage_name = cfg$stage_name
  )

  tmp <- tempfile()
  df2 <- data.frame(Y = 'y2', X = 'x2')
  readr::write_tsv(df2, tmp)
  db_load_from_file(
    src$con,
    table_name = table_name,
    file_path = tmp,
    stage_dir = cfg$stage_name,
    from_colnames = colnames(df2),
    format_opts = list(FIELD_DELIMITER = "'\t'")
  )

  actual <- t %>%
    collect() %>%
    arrange(X)
  expect_equal(
    actual,
    tibble(
      X = c('x1', 'x2'),
      Y = c('y1', 'y2')
    )
  )
})
