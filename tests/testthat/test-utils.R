devtools::load_all()

test_that('make_sql_col_idx_exprs() works', {
  to_colnames <- c('X', 'Y')
  from_colnames <- c('Y', 'X')

  actual <- make_sql_col_idx_exprs(
    to_colnames,
    from_colnames
  )

  expect_equal(
    actual,
    c('$2', '$1')
  )
})
