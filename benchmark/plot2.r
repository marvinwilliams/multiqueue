library(ggplot2)
library(dplyr)
library(reshape2)
library(stringr)

experiment_dir <- "./experiments"

read_histogram <- function(file, name) {
  data <- read.csv(file = file, header = F, col.names = c("rank", "frequency"), sep = " ")
  data$name <- name
  data["cummulated"] <- rev(cumsum(rev(data$frequency)))
  data$cummulated <- data$cummulated / data$cummulated[1]
  data
}

read_throughput <- function(filename) {
  read.csv(file = filename, header = T, sep = " ")
}

read_histogramlist <- function(files, prefill, dist) {
  hist_list <- list()
  i <- 1
  for (f in files) {
    data <- read_histogram(f, gsub(".txt", "", gsub(".*/", "", f)))
    print(data)
    hist_list[[i]] <- data
    i <- i + 1
  }
  data <- do.call(rbind, hist_list)
  data$prefill <- prefill
  data$dist <- dist
}

read_throughputlist <- function(files, prefill, dist) {
  throughput_list <- list()
  i <- 1
  for (f in files) {
    data <- read_throughput(f)
    data <- data %>%
      group_by(threads) %>%
      summarize(mean = mean(throughput / 1000000), sd = sd(throughput / 1000000))
    data$name <- gsub(".txt", "", gsub(".*/", "", f))
    throughput_list[[i]] <- data
    i <- i + 1
  }
  data <- do.call(rbind, throughput_list)
  data$prefill <- prefill
  data$dist <- dist
}

read_scenario <- function(scenario) {
  scenario_list <- list()
  i <- 1
  for (exp in list.dirs(path = paste(experiment_dir, scenario, sep = "/"), full.names = T, recursive = F)) {
    config <- read.csv(file = paste(exp, "config", sep = "/"), header = T)
    print(exp)
    data <- read_histogramlist(list.files(path = exp, pattern = "txt$", full.names = T), config$prefill[1], config$dist[1])
    scenario_list[[i]] <- data
    i <- i + 1
  }
  do.call(rbind, scenario_list)
}

plot_rank_histogram <- function(data, outdir) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Rank") +
    geom_line() +
    facet_wrap(~ prefill + dist) +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/rank_plot.pdf", sep = ""))
}

plot_delay_histogram <- function(data, outdir) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Delay") +
    geom_line() +
    facet_wrap(~ prefill + dist) +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/delay_plot.pdf", sep = ""))
}

plot_top_delay_bar <- function(data, outdir) {
  transformed <- data %>%
    group_by(name) %>%
    summarize(mean = weighted.mean(rank, frequency), max = max(rank))
  transformed <- melt(transformed, id.vars = c("name", "prefill", "dist"), measure.vars = c("mean", "max"))
  plot <- ggplot(transformed, aes(x = as.factor(name), y = value, fill = name)) +
    labs(x = "Priority Queue", y = "Top Delay", title = "Top Delay") +
    geom_bar(stat = "identity") +
    facet_wrap(~ variable + prefill + dist, scales = "free") +
    theme(axis.ticks.x = element_blank(), axis.text.x = element_blank())
  ggsave(plot, file = paste(outdir, "/top_delay_plot.pdf", sep = ""))
}

plot_throughput_by_thread <- function(data, outdir) {
  plot <- ggplot(data, aes(x = threads, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~ prefill + dist) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_plot.pdf", sep = ""))
}

plot_throughput_by_prefill <- function(data, outdir) {
  plot <- ggplot(subset(data, threads = 8), aes(x = prefill, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~dist) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_prefill_plot.pdf", sep = ""))
}

plot_throughput_by_dist <- function(data, outdir) {
  plot <- ggplot(subset(data, threads = 8), aes(x = as.factor(dist), y = mean, group = name, color = name)) +
    geom_bar(stat = "identity") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~prefill) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_dist_plot.pdf", sep = ""))
}


plot_scenario <- function(data, dir) {
  plot_rank_histogram(data, dir)
  plot_delay_histogram(data, dir)
  plot_top_delay_bar(data, dir)
  plot_throughput_by_thread(data, dir)
}

data <- read_scenario("prefill")
plot_scenario(data, paste(experiment_dir, "prefill", sep = "/"))
plot_throughput_by_prefill(data, paste(experiment_dir, "prefill", sep = "/"))

data <- read_scenario("distribution")
plot_scenario(data, paste(experiment_dir, "distribution", sep = "/"))
plot_throughput_by_dist(data, paste(experiment_dir, "distribution", sep = "/"))
