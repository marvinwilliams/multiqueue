library(plyr)
library(dplyr)
library(tidyverse)
library(ggplot2)


readhistogram <- function(name) {
  tab <- read.table(file=name, header=F, col.names=c('x', 'y'))
  tab$y <- rev(cumsum(rev(tab$y)))
  tab$y <- tab$y / tab$y[1]
  tab
}

read.histogramtables <- function(file.names, ...) {
  ldply(file.names, function(fn) data.frame(mode=gsub(".txt", "", gsub(".*/", "", fn)), readhistogram(fn, ...)))
}

read.runtimetables <- function(file.names, ...) {
  ldply(file.names, function(fn) data.frame(pq=gsub(".txt", "", gsub(".*/", "", fn)), read.table(file=fn, header=F, col.names=c('x', 'y'))))
}


ranks <- read.histogramtables(paste("rank_histograms", list.files(path = "rank_histograms", pattern = "*.txt"), sep="/"))
rplot <- ggplot(ranks, aes(x=x, y=y, group=mode, color=mode)) + geom_line() + labs(x = "Rank", y = "Cummul. Frequency", title = "Rank, p = 8")
delays <- read.histogramtables(paste("delay_histograms", list.files(path = "delay_histograms", pattern = "*.txt"), sep="/"))
dplot <- ggplot(delays, aes(x=x, y=y, group=mode, color=mode)) + geom_line() + labs(x = "Delay", y = "Cummul. Frequency", title = "Delay, p = 8")
tdelays <- read.histogramtables(paste("top_delay_histograms", list.files(path = "top_delay_histograms", pattern = "*.txt"), sep="/"))
tdplot <- ggplot(tdelays, aes(x=x, y=y, group=mode, color=mode)) + geom_line() + labs(x = "Top Delay", y = "Cummul. Frequency", title = "Top Delay, p = 8")
runtimes <- read.runtimetables(paste("runtimes", list.files(path = "runtimes", pattern = "*.txt"), sep="/"))
rtplot <- ggplot(runtimes, aes(x=x, y=y/1000000, group=pq, color=pq)) + geom_line() + geom_point() + labs(x = "p", y = "MOps/s", title = "Operations per second")
pdf("plots.pdf")
print(rplot)
print(dplot)
print(tdplot)
print(rtplot)
dev.off()

