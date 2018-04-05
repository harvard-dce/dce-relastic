
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
  tbl_df(res)
}

getEpisodes <- function(series = NULL, year.term = NULL) {

  if (is.null(series) && is.null(year.term)) {
    stop("need either series or year.term arg")
  }

  if (!is.null(year.term)) {
    yt <- splitYearTerm(year.term)
  } else {
    yt <- yearTermFromSeries(series)
  }

  must <- list(
    list(term=list(year=yt$year)),
    list(term=list(term=yt$term))
  )
  if (!is.null(series)) {
    must <- c(must, list(list(term=list(series=series))))
  }
  q <- list(
    query=list(
      bool=list(
        must=must
      )
    )
  )
  qbody <- toJSON(q, pretty = T, auto_unbox = T)
  res <- Search(es.episode.index, body = qbody, asdf = TRUE, size = 9999)
  res <- res$hits$hits
  # rewrite the source doc field names to make the df easier to work with
  res <- res %>% setNames(gsub("_source\\.", "", names(.)))
  tbl_df(res)
}

transcriptStats <- function() {
  qbody <- '{
    "query": { "match_all": {} },
    "aggs": {
      "per_lecture": {
        "terms": {
          "field": "mpid",
          "size": 0
        },
        "aggs": {
          "confidence": {
            "extended_stats": {
              "field": "confidence"
            }
          },
          "hesitations": {
            "sum": {
              "field": "hesitations"
            }
          },
          "words": {
            "sum": {
              "field": "word_count"
            }
          },
          "hesitations_length": {
            "sum": {
              "field": "hesitation_length"
            }
          }
        }
      }
    }
  }'
  res <- Search(es.transcript.index, body = qbody, asdf = T, size = 0)
  tbl_df(data.frame(res$aggregations))
}

histogramAgg <- function(field, interval, agg.name = NULL, min_doc_count = 0) {
  agg <- list()
  if (is.null(agg.name)) {
    agg.name <- sprintf("%s_histogram", field)
  }
  agg[[agg.name]] <- list(
    histogram=list(
      field=field,
      interval=interval,
      min_doc_count=min_doc_count
    )
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

termAgg <- function(field, size = 0, sub_aggs = NULL) {
  agg <- list()
  agg.name <- sprintf("by_%s", field)
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

viewingStats <- function(index, year.term = NULL, aggs = NULL, series = NULL, huid = NULL,
                         mpid = NULL, live = NULL, playing = T, anonymous = F, print.json = F) {

  must_not <- list()
  must <- list()

  if (playing) {
    must <- c(must, list(list(term=list(action.is_playing=T))))
  }

  if (!anonymous) {
    must_not <- c(must_not, list(list(term=list(huid="anonymous"))))
  }

  if (!is.null(year.term)) {
    yt <- splitYearTerm(year.term)
    must <- c(
      must,
      list(list(term=list(episode.year=yt$year))),
      list(list(term=list(episode.term=yt$term)))
    )
  }
  if (!is.null(series)) {
    must <- c(must, list(list(term=list(episode.series=series))))
  }
  if (!is.null(huid)) {
    must <- c(must, list(list(term=list(huid=huid))))
  }
  if (!is.null(mpid)) {
    must <- c(must, list(list(term=list(mpid=mpid))))
  }
  if (!is.null(live)) {
    must <- c(must, list(list(term=list(is_live=as.integer(live)))))
  }

  q <- list(
    query=list(
      bool=list(
        must=must,
        must_not=must_not
      )
    )
  )

  if (!is.null(aggs)) {
    q$aggs <- aggs
  }

  qbody <- toJSON(q, pretty = T, auto_unbox = T)
  if (print.json) {
    print(qbody)
  }
  res <- Search(index, body = qbody, asdf = T, size = 0)
  if (res$hits$total) {
    tbl_df(data.frame(res$aggregations))
  } else {
    tibble()
  }
}
