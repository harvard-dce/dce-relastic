
es <- connect(
  es_host = es.host,
  es_port = es.port,
  es_transport_schema = es.transport,
  es_path = es.path,
  es_user = es.user,
  es_pwd = es.pwd
)

tryCatch({
  es.info <- elastic::info()
  print("Connected to elasticsearch")
}, error = function(ex) {
  print("Unable to communicate with Elasticsearch");
  stop(ex)
})


splitYearTerm <- function(year.term) {
  yt <- str_split(year.term, "-", simplify = T)
  YT <- data.frame(year=NA, term=NA)
  YT[, c("year", "term")] <- yt
  YT
}

yearTermFromSeries <- function(series) {
  data.frame(year=substr(series, 1, 4), term=substr(series, 5, 6))
}

matchAllQuery <- function() {
  list(
    query=list(
      match_all=fromJSON()
    )
  )
}

queryToJson <- function(q) {
  qbody <- toJSON(q, pretty = T, auto_unbox = T)
  if (es.query.debug) {
    print(qbody)
  }
  qbody
}

episodesByTerm <- function(year.term) {
  yt <- splitYearTerm(year.term)
  qbody <- str_interp('{
    "query": {
      "bool": {
        "must": [
             { "term": { "year": "${yt$year}" }},
             { "term": { "term": "${yt$term}" }}
        ]
      }
    }
  }')
  res <- Search(es.episode.index, body = qbody, asdf = TRUE, size = 9999)
  res <- res$hits$hits
  # rewrite the source doc field names to make the df easier to work with
  res <- res %>% setNames(gsub("_source\\.", "", names(.)))
  as.tibble(res)
}

getEpisode <- function(mpid) {
  q <- list(
    query=list(
      bool=list(
        filter=list(
          termFilter("mpid", mpid)
        )
      )
    )
  )
  qbody <- toJSON(q, pretty = T, auto_unbox = T)
  print(qbody)
  res <- Search(es.episode.index, body = qbody, asdf = TRUE, size = 1)
  res <- res$hits$hits %>% setNames(gsub("_source\\.", "", names(.)))
  as.tibble(res)
}

getEpisodes <- function(series = NULL, year.term = NULL, qfilter = list(), must_not = list()) {

  if (is.null(series) && is.null(year.term)) {
    stop("need either series or year.term arg")
  }

  if (!is.null(year.term)) {
    yt <- splitYearTerm(year.term)
  } else {
    yt <- yearTermFromSeries(series)
  }

  qfilter <- c(qfilter, list(
    list(term=list(year=yt$year)),
    list(term=list(term=yt$term))
  ))

  if (!is.null(series)) {
    qfilter <- c(qfilter, list(list(term=list(series=series))))
  }

  q <- list(
    query=list(
      bool=list(
        filter=qfilter,
        must_not=must_not
      )
    )
  )

  qbody <- queryToJson(q)
  res <- Search(es.episode.index, body = qbody, asdf = TRUE, size = 9999)
  # rewrite the source doc field names to make the df easier to work with
  res <- res$hits$hits %>% setNames(gsub("_source\\.", "", names(.)))
  as.tibble(res)
}

transcriptStats <- function() {
  q <- matchAllQuery()
  q$aggs <- termAgg(
    "mpid",
    agg.name = "per_lecture",
    sub_aggs = c(
      metricAgg("confidence", "extended_stats", agg.name = "confidence"),
      metricAgg("hesitations", "sum", agg.name = "hesitations"),
      metricAgg("word_count", "sum", agg.name = "word_count"),
      metricAgg("hesitations_length", "sum", agg.name = "hesitations_length")
    )
  )
  qbody <- queryToJson(q)
  res <- Search(es.transcript.index, body = qbody, asdf = T, size = 0)
  as.tibble(data.frame(res$aggregations))
}

metricAgg <- function(field, type, agg.name = NULL) {
  agg <- list()
  if (is.null(agg.name)) {
    agg.name <- sprintf("%s_%s", field, type)
  }
  agg[[agg.name]] <- list()
  agg[[agg.name]][[type]] <- list(
    field=field
  )
  agg
}

histogramAgg <- function(field, interval, type = "histogram", agg.name = NULL, min_doc_count = 0) {
  agg <- list()
  if (is.null(agg.name)) {
    agg.name <- sprintf("%s_histogram", field)
  }
  agg[[agg.name]] <- list()
  agg[[agg.name]][[type]] <- list(
    field=field,
    interval=interval,
    min_doc_count=min_doc_count
  )
  agg
}

cardinalityAgg <- function(field) {
  agg <- list()
  agg.name <- sprintf("%s_cardinality", field)
  agg[[agg.name]] <- list(
    cardinality=list(
      field=field
    )
  )
  agg
}

termAgg <- function(field, size = 0, agg.name = NULL, sub_aggs = NULL) {
  agg <- list()

  if (is.null(agg.name)) {
    agg.name <- sprintf("by_%s", field)
  }

  agg[[agg.name]] <- list(
    terms=list(
      field=field,
      size=size
    )
  )
  if (!is.null(sub_aggs)) {
    agg[[agg.name]]$aggs <- sub_aggs
  }
  agg
}

termFilter <- function(field, value) {
  tf <- list(term=list())
  tf$term[field] <- value
  tf
}

studentWatchScore <- function(es.actions.index, huid, mpid) {
  qfilter <- list(
    termFilter("huid", huid),
    termFilter("mpid", mpid),
    termFilter("action.type", "HEARTBEAT"),
    termFilter("action.is_playing", T)
  )
  q <- list(
    query=list(
      bool=list(
        filter=qfilter
      )
    )
  )
  q$aggs <- histogramAgg("action.inpoint", 300, agg.name = "inpoints", min_doc_count = 1)
  qbody <- toJSON(q, pretty = T, auto_unbox = T)
  if (params$debug) {
    print(qbody)
  }
  res <- Search(es.actions.index, body = qbody, asdf = T, size = 0)
  if (res$hits$total) {
    as.tibble(data.frame(res$aggregations))
  } else {
    tibble()
  }
}

episodeStats <- function(mpid, duration, live = F) {

  aggs <- c(
    histogramAgg(
      "@timestamp",
      "day",
      type = "date_histogram",
      agg.name = "by_day"
    ),
    termAgg("huid",
      sub_aggs = histogramAgg(
        "action.inpoint",
        inpoint.interval,
        agg.name = "inpoints"
      )
    )
  )

  actions.res <- getActions(
    mpid = mpid,
    live = live,
    qfilter = list(termFilter("action.type", "HEARTBEAT")),
    aggs = aggs
  )

  if (!actions.res$hits$total) {
    list(NULL)
  } else {
    inpoints <- as.tibble(data.frame(actions.res$aggregations$by_huid))
#    days <- as.tibble(data.frame(actions.res$aggregations$by_day))
#    days <- list(
#      timestamps=actions.res$aggregations$by_day$buckets$key,
#      event.count=actions.res$aggregations$by_day$buckets$doc_count
#    )

    duration.in.seconds <- round(duration / 1000)
    total.intervals <- ceiling(duration.in.seconds / inpoint.interval)

    inpoints <- inpoints %>%
      rowwise() %>%
      mutate(
        huids             = buckets.key,
        watched.intervals = nrow(buckets.inpoints.buckets),
        watched.pct       = (watched.intervals / total.intervals) * 100
      )

    list(
      list(
        mean.watched.pct = round(mean(inpoints$watched.pct), 2),
        unique.viewers   = length(inpoints$huids),
        timestamps       = actions.res$aggregations$by_day$buckets$key,
        event.count      = actions.res$aggregations$by_day$buckets$doc_count
      )
    )
  }
}

getActions <- function(year.term = NULL, series = NULL, huid = NULL,
                       mpid = NULL, live = NULL, playing = T,  anonymous = F,
                       qfilter = list(), must_not = list(), aggs = NULL,
                       size = 0, print.json = F) {
  if (playing) {
    qfilter <- c(qfilter, list(list(term=list(action.is_playing=T))))
  }

  if (!anonymous) {
    must_not <- c(must_not, list(list(terms=list(huid=c("anonymous", "public_user")))))
  }

  if (!is.null(year.term)) {
    yt <- splitYearTerm(year.term)
    qfilter <- c(
      qfilter,
      list(list(term=list(episode.year=yt$year))),
      list(list(term=list(episode.term=yt$term)))
    )
  }
  if (!is.null(series)) {
    qfilter <- c(qfilter, list(list(term=list(episode.series=series))))
  }
  if (!is.null(huid)) {
    qfilter <- c(qfilter, list(list(term=list(huid=huid))))
  }
  if (!is.null(mpid)) {
    qfilter <- c(qfilter, list(list(term=list(mpid=mpid))))
  }
  if (!is.null(live)) {
    qfilter <- c(qfilter, list(list(term=list(is_live=as.integer(live)))))
  }

  q <- list(
    query=list(
      bool=list(
        filter=qfilter,
        must_not=must_not
      )
    )
  )

  if (!is.null(aggs)) {
    q$aggs <- aggs
  }

  qbody <- queryToJson(q)
  Search(es.actions.index, body = qbody, asdf = T, size = 0)
}
