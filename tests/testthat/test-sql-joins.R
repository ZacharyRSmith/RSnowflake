# Copyright 2017 Snowflake Computing Inc.
# (derived from dplyr, Copyright 2013-2017 RStudio)
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

suppressMessages({
  library(lubridate)
  library(testthat)
  devtools::load_all()
  options(dplyr.jdbc.classpath = Sys.getenv('SNOWFLAKE_JAR'))
})

`%||na%` <- function(a, b) if (is.null(a) || is.na(a)) b else a

cfg <- list(
  stage_name = Sys.getenv('DPLYR_SNOWFLAKEDB_TESTS_STAGE'),
  src_type = Sys.getenv('DPLYR_SNOWFLAKEDB_TESTS_SRC_TYPE', unset = NA) %||na% 'snowflakedb'
)
if (cfg$stage_name == '') stop('cfg$stage_name must be populated')
flog.threshold(DEBUG)


context("SQL: joins") # TODO: mv other tests

src <- if (cfg$src_type == 'sqlite') {
  src_sqlite(tempfile(), create = TRUE)
} else if (cfg$src_type == 'snowflakedb') {
  src_snowflakedb(account = Sys.getenv("SNOWFLAKE_ACCOUNT"),
                  host = Sys.getenv("SNOWFLAKE_HOST"),
                  user = Sys.getenv("SNOWFLAKE_USER"),
                  password = Sys.getenv("SNOWFLAKE_PASSWORD"),
                  opts = list(warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                              db = Sys.getenv("SNOWFLAKE_DB"),
                              role = Sys.getenv("SNOWFLAKE_ROLE"),
                              schema = "public",
                              tracing = "off"))
} else {
  stop(glue::glue('cfg$src_type="{cfg$src_type}" not supported'))
}

src <- extend_src(src)

dez_names <- list(
  # copy_to() requires table name to be fully qualified when connection type is snowflake.
  df1 = if (cfg$src_type == 'snowflakedb') 'SANDBOX.PUBLIC.DF1' else 'df1',
  df2 = if (cfg$src_type == 'snowflakedb') 'SANDBOX.PUBLIC.DF2' else 'df2',
  fam = if (cfg$src_type == 'snowflakedb') 'SANDBOX.PUBLIC.FAM' else 'fam'
)

df1_orig <- data.frame(X = 1:5, Y = 1:5)
df1 <- copy_to(
  dest = src,
  df = df1_orig,
  name = dez_names$df1,
  temporary = TRUE,
  stage_name = cfg$stage_name
)
df2_orig <- data.frame(A = 5:1, B = 1:5)
df2 <- copy_to(
  src,
  df2_orig,
  dez_names$df2,
  temporary = TRUE,
  stage_name = cfg$stage_name
)
fam_orig <- data.frame(ID = 1:5, PARENT = c(NA, 1, 2, 2, 4))
fam <- copy_to(
  src,
  fam_orig,
  dez_names$fam,
  temporary = TRUE,
  stage_name = cfg$stage_name
)

test_that("get_query works", {
  df1 <- src$get_query(
    "select *
    from df1
    limit 4"
  )
  expect_equal(names(df1), colnames(df1_orig))
  expect_equal(nrow(df1), 4)
})

test_that("get_tbl works", {
  df1 <- src$get_tbl("df1") %>%
    collect()
  expect_equal(names(df1), colnames(df1_orig))
  expect_equal(nrow(df1), 5)
})

test_that("named by join by different x and y vars", {

  j1 <- collect(inner_join(df1, df2, c("X" = "A")))
  expect_equal(names(j1), c("X", "Y", "B"))
  expect_equal(nrow(j1), 5)

  j2 <- collect(inner_join(df1, df2, c("X" = "A", "Y" = "B")))
  expect_equal(names(j2), c("X", "Y"))
  expect_equal(nrow(j2), 1)
})

test_that("self-joins allowed with named by", {
  j1 <- collect(left_join(fam, fam, by = c("PARENT" = "ID")))
  j2 <- collect(inner_join(fam, fam, by = c("PARENT" = "ID")))

  expect_equal(names(j1), c("ID", "PARENT.x", "PARENT.y"))
  expect_equal(names(j2), c("ID", "PARENT.x", "PARENT.y"))
  expect_equal(nrow(j1), 5)
  expect_equal(nrow(j2), 4)

  j3 <- collect(semi_join(fam, fam, by = c("PARENT" = "ID")))
  j4 <- collect(anti_join(fam, fam, by = c("PARENT" = "ID")))

  expect_equal(j3, filter(collect(fam), !is.na(PARENT)))
  expect_equal(j4, filter(collect(fam), is.na(PARENT)))
})

test_that("copy_into works", {
  skip_if(cfg$src_type == 'sqlite')
  DF <- data.frame(
    logical = c(NA, TRUE, FALSE),
    integer = 1L:3L,
    numeric = 1:3,
    character = c('a', 'b', 'c'),
    Date = c(
      lubridate::as_date('2000-01-01'),
      lubridate::as_date('2000-01-02'),
      lubridate::as_date('2000-01-03')
    ),
    POSIXct = c(
      lubridate::as_datetime('2000-01-01'),
      lubridate::as_datetime('2000-01-02'),
      lubridate::as_datetime('2000-01-03')
    ),
    stringsAsFactors = FALSE
  )

  q <- db_create_table(
    con = src$con,
    table = 'SANDBOX.PUBLIC.BUZZ',
    types = db_data_type(src$con, DF),
    temporary = FALSE,
    primary_key = c('integer', 'numeric'),
    replace = TRUE
  )

  copy_to(
    dest = src,
    df = DF,
    name = 'SANDBOX.PUBLIC.BUZZ',
    mode = 'overwrite',
    stage_name = cfg$stage_name
  )
  
  t <- src$get_tbl('SANDBOX.PUBLIC.BUZZ')
  t_cols <- t$columns()
  expect_equal(
    t_cols$DATA_TYPE,
    c(
      "BOOLEAN",
      "NUMBER",
      "NUMBER",
      "TEXT",
      "DATE",
      "TIMESTAMP_NTZ"
    )
  )
  expect_equal(
    t$collect_and_cast(),
    local({
      colnames(DF) <- toupper(colnames(DF))
      DF
    })
  )
})
