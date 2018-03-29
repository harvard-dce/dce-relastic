
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

viewingStats <- function(index, year.term = NULL, aggs = NULL, series = NULL,
                         huid = NULL, mpid = NULL, playing = T, anonymous = F) {

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
  res <- Search(index, body = qbody, asdf = T, size = 0)
  tbl_df(data.frame(res$aggregations))
}
