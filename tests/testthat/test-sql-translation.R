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

library(testthat)
# library(dplyr.snowflakedb)
devtools::load_all()
options(dplyr.jdbc.classpath = Sys.getenv("SNOWFLAKE_JAR"))

context("SQL translation")

src_type <- Sys.getenv('DPLYR_SNOWFLAKEDB_TESTS_SRC_TYPE')
src_type <- if (src_type == '') 'snowflakedb' else src_type
src <- if (src_type == 'sqlite') {
  src_sqlite(tempfile(), create = TRUE)
} else if (src_type == 'snowflakedb') {
  src_snowflakedb(account = Sys.getenv("SNOWFLAKE_ACCOUNT"),
                  host = Sys.getenv("SNOWFLAKE_HOST"),
                  user = Sys.getenv("SNOWFLAKE_USER"),
                  password = Sys.getenv("SNOWFLAKE_PASSWORD"),
                  opts = list(warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                              db = Sys.getenv("SNOWFLAKE_DB"),
                              schema = "public",
                              tracing = "off"))
} else {
  stop(glue::glue('src_type="{src_type}" not supported'))
}

test_that("Simple maths is correct", {
  expect_equal(translate_sql(1 + 2), sql("1.0 + 2.0"))
  expect_equal(translate_sql(2 * 4), sql("2.0 * 4.0"))
  expect_equal(translate_sql(5 ^ 2), sql("POWER(5.0, 2.0)"))
  expect_equal(translate_sql(100L %% 3L), sql("100 % 3"))
})

test_that("dplyr.strict_sql = TRUE prevents auto conversion", {
  old <- options(dplyr.strict_sql = TRUE)
  on.exit(options(old))

  expect_equal(translate_sql(1 + 2), sql("1.0 + 2.0"))
  expect_error(translate_sql(blah(x)), "could not find function")
})

test_that("Wrong number of arguments raises error", {
  expect_error(translate_sql(mean(1, 2, na.rm = TRUE), window = FALSE), "unused argument")
})

test_that("Named arguments (other than `x`) raises error", {
  expect_error(translate_sql(mean(y = 2, na.rm = TRUE), window = FALSE), "unused argument")
})

test_that("Subsetting always evaluated locally", {
  x <- list(a = 1, b = 1)
  y <- c(2, 1)

  correct <- quote(`_var` == 1)

  expect_equal(partial_eval(quote(`_var` == x$a)), correct)
  expect_equal(partial_eval(quote(`_var` == x[[2]])), correct)
  expect_equal(partial_eval(quote(`_var` == y[2])), correct)
})

test_that("between translated to special form (#503)", {

  out <- translate_sql(between(x, 1, 2))
  expect_equal(out, sql('"x" BETWEEN 1.0 AND 2.0'))
})

test_that("is.na and is.null are equivalent",{
  expect_equal(translate_sql(!is.na(x)), sql('NOT((("x") IS NULL))'))
  expect_equal(translate_sql(!is.null(x)), sql('NOT((("x") IS NULL))'))
})

test_that("if translation adds parens", {
  expect_equal(
    translate_sql(if (x) y),
    sql('CASE WHEN ("x") THEN ("y") END')
  )
  expect_equal(
    translate_sql(if (x) y else z),
    sql('CASE WHEN ("x") THEN ("y") WHEN NOT("x") THEN ("z") END')
  )

})

test_that("pmin and pmax become min and max", {
  expect_equal(translate_sql(pmin(x, y)), sql('MIN("x", "y")'))
  expect_equal(translate_sql(pmax(x, y)), sql('MAX("x", "y")'))
})

# Minus -------------------------------------------------------------------

test_that("unary minus flips sign of number", {
  expect_equal(translate_sql(-10L), sql("-10"))
  expect_equal(translate_sql(x == -10), sql('"x" = -10.0'))
  expect_equal(translate_sql(x %in% c(-1L, 0L)), sql('"x" IN (-1, 0)'))
})

test_that("unary minus wraps non-numeric expressions", {
  expect_equal(translate_sql(-(1L + 2L)), sql("-(1 + 2)"))
  expect_equal(translate_sql(-mean(x, na.rm = TRUE), window = FALSE), sql('-AVG("x")'))
})

test_that("binary minus subtracts", {
  expect_equal(translate_sql(1L - 10L), sql("1 - 10"))
})

# Window functions --------------------------------------------------------

test_that("window functions without group have empty over", {
  expect_equal(translate_sql(n()), sql("COUNT(*) OVER ()"))
  expect_equal(translate_sql(sum(x, na.rm = TRUE)), sql('sum("x") OVER ()'))
})

test_that("aggregating window functions ignore order_by", {
  expect_equal(
    translate_sql(n(), vars_order = "x"),
    sql("COUNT(*) OVER ()")
  )
  expect_equal(
    translate_sql(sum(x, na.rm = TRUE), vars_order = "x"),
    sql('sum("x") OVER ()')
  )
})

test_that("cumulative windows warn if no order", {
  expect_warning(translate_sql(cumsum(x)), "does not have explicit order")
  expect_warning(translate_sql(cumsum(x), vars_order = "x"), NA)
})

test_that("ntile always casts to integer", {
  expect_equal(
    translate_sql(ntile(x, 10.5)),
    sql('NTILE(10) OVER (ORDER BY "x")')
  )
})
