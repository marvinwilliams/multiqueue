library(ggplot2)
library(dplyr)
library(reshape2)
library(stringr)

outdir <- "./plots"
# mkdir(outdir)
throughput_dir <- "./throughput"
rank_dir <- "./rank"
delay_dir <- "./delay"
top_delay_dir <- "./top_delay"

read_histogram <- function(filename) {
  data <- read.csv(file = filename, header = F, col.names = c("rank", "frequency"), sep = " ")
  data$name <- gsub(".txt", "", gsub(".*/", "", filename))
  data["cummulated"] <- rev(cumsum(rev(data$frequency)))
  data$cummulated <- data$cummulated / data$cummulated[1]
  data
}

read_throughput <- function(filename) {
  read.csv(file = filename, header = T, sep = " ")
}

read_histogramlist <- function(files, ...) {
  hist_list <- list()
  i <- 1
  for (f in files) {
    data <- read_histogram(f)
    hist_list[[i]] <- data
    i <- i + 1
  }
  do.call(rbind, hist_list)
}

read_throughputlist <- function(files, ...) {
  throughput_list <- list()
  i <- 1
  for (f in files) {
    data <- read_throughput(f)
    data$name <- gsub(".txt", "", gsub(".*/", "", f))
    sum_data <- data %>%
      group_by(threads) %>%
      summarize(mean = mean(throughput) / 1000000, sd = sd(throughput / 1000000))
    sum_data$name <- data$name[1:nrow(sum_data)]
    throughput_list[[i]] <- sum_data
    i <- i + 1
  }
  do.call(rbind, throughput_list)
}

plot_rank_histogram <- function(data) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Rank") +
    geom_line() +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/rank_plot.pdf", sep = ""))
}

plot_delay_histogram <- function(data) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Delay") +
    geom_line() +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/delay_plot.pdf", sep = ""))
}

plot_top_delay_bar <- function(data) {
  transformed <- data %>%
    group_by(name) %>%
    summarize(mean = weighted.mean(rank, frequency), max = max(rank))
  transformed <- melt(transformed, id = 1, measure = 2:3)
  plot <- ggplot(transformed, aes(x = as.factor(name), y = value, fill = name)) +
    labs(x = "Priority Queue", y = "Top Delay", title = "Top Delay") +
    geom_bar(stat = "identity") +
    facet_wrap(~variable, scales = "free") +
    theme(axis.ticks.x = element_blank(), axis.text.x = element_blank())
  ggsave(plot, file = paste(outdir, "/top_delay_plot.pdf", sep = ""))
}

plot_throughput <- function(data) {
  plot <- ggplot(data, aes(x = threads, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    )
  labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_plot.pdf", sep = ""))
}


data <- read_histogramlist(paste(rank_dir, list.files(path = rank_dir, pattern = "*.txt"), sep = "/"))
plot_rank_histogram(data)

data <- read_histogramlist(paste(delay_dir, list.files(path = delay_dir, pattern = "*.txt"), sep = "/"))
plot_delay_histogram(data)

data <- read_histogramlist(paste(top_delay_dir, list.files(path = top_delay_dir, pattern = "*.txt"), sep = "/"))
plot_top_delay_bar(data)

data <- read_throughputlist(paste(throughput_dir, list.files(path = throughput_dir, pattern = "*.txt"), sep = "/"))
plot_throughput(data)
