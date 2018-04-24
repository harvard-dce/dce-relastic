#rm(list=ls())

library(dotenv)
library(jsonlite)
library(tidyverse)
library(elastic)
library(anonymizer)

source("config.R")
source("es.R")
source("util.R")
