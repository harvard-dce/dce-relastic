
csv2tibble <- function(path, pattern = NULL) {
  files <- dir(path, pattern, full.names = T)
  df <- do.call(rbind,lapply(files,read.csv))
  as.tibble(df)
}
