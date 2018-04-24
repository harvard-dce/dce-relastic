
dotenv::load_dot_env(file = ".env")

env <- function(key, default=NA) {
  val <- Sys.getenv(key)
  if (val == "" && !is.na(default)) {
    val <- default
  }
  val
}

es.host <- env("ES_HOST", 'localhost')
es.port <- env("ES_PORT", '9200')
es.transport <- env("ES_TRANSPORT", 'http')
es.path <- env("ES_PATH")
es.user <- env("ES_USER")
es.pwd <- env("ES_PWD")
es.actions.index <- env("ES_ACTIONS_INDEX", 'useractions-*')
es.episode.index <- env("ES_EPISODE_INDEX", 'episodes')
es.transcript.index <- env("ES_TRANSCRIPT_INDEX", 'transcripts')
es.query.debug <- F
default.tz <- "America/New_York"
rollcall.path <- env('ROLLCALL_PATH')
inpoint.interval <- 300
campus.location <- list(lat=42.3582, lon=-71.0507)
