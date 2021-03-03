library(ggplot2)
library(dplyr)
library(reshape2)
library(stringr)

experiment_dir <- "./experiments_i10pc133"

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
  if (length(files) == 0) {
    data <- data.frame(matrix(ncol = 6, nrow = 0))
    colnames(data) <- c("rank", "frequency", "name", "cummulated", "prefill", "dist")
    return(data)
  }
  hist_list <- list()
  i <- 1
  for (f in files) {
    data <- read_histogram(f, gsub(".txt", "", gsub(".*/", "", f)))
    hist_list[[i]] <- data
    i <- i + 1
  }
  data <- do.call(rbind, hist_list)
  data$prefill <- prefill
  data$dist <- dist
  return(data)
}

read_throughputlist <- function(files, prefill, dist) {
  if (length(files) == 0) {
    data <- data.frame(matrix(ncol = 6, nrow = 0))
    colnames(data) <- c("threads", "mean", "sd", "name", "prefill", "dist")
    return(data)
  }
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
  return(data)
}


plot_rank_histogram <- function(data, outdir) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Rank") +
    geom_line() +
    scale_color_brewer(palette="Set1") +
    facet_grid(rows = vars(prefill, dist)) +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/rank_plot.pdf", sep = ""))
}

plot_delay_histogram <- function(data, outdir) {
  plot <- ggplot(data, aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Delay") +
    geom_line() +
    scale_color_brewer(palette="Set1") +
    facet_grid(rows = vars(prefill, dist)) +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/delay_plot.pdf", sep = ""))
}

plot_top_delay_bar <- function(data, outdir) {
  transformed <- data %>%
    group_by(name, prefill, dist) %>%
    summarize(mean = weighted.mean(rank, frequency), max = max(rank), .groups = "keep")
  transformed <- melt(transformed, measure.vars = c("mean", "max"))
  plot <- ggplot(transformed, aes(x = as.factor(name), y = value, fill = name)) +
    labs(x = "Priority Queue", y = "Top Delay", title = "Top Delay") +
    geom_bar(stat = "identity") +
    scale_color_brewer(palette="Set1") +
    # facet_grid(cols = vars(variable), rows = vars(prefill, dist), scales = "free") +
    facet_grid(rows = vars(variable), cols = vars(prefill, dist), scales = "free") +
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
    facet_grid(rows = vars(prefill, dist)) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_plot.pdf", sep = ""))
}

plot_throughput_by_prefill <- function(data, outdir) {
  print(subset(data, threads == 8))
  plot <- ggplot(subset(data, threads == 8), aes(x = prefill, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_grid(rows = vars(dist)) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_prefill_plot.pdf", sep = ""))
}

plot_throughput_by_dist <- function(data, outdir) {
  plot <- ggplot(data, aes(x = threads, y = mean, group = name, color = name, fill = name)) +
    geom_line() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    scale_color_brewer(palette="Set1") +
    facet_grid(rows = vars(dist)) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_dist_plot.pdf", sep = ""))
}


plot_scenario <- function(data, dir) {
   # plot_rank_histogram(data$rank, dir)
   # plot_delay_histogram(data$delay, dir)
   plot_top_delay_bar(data$top_delay, dir)
   # plot_throughput_by_thread(data$throughput, dir)
}

read_scenario <- function(scenario) {
  scenario_data <- list()
  rank_list <- list()
  delay_list <- list()
  top_delay_list <- list()
  throughput_list <- list()
  i <- 1
  for (exp in list.dirs(path = paste(experiment_dir, scenario, sep = "/"), full.names = T, recursive = F)) {
    config <- read.csv(file = paste(exp, "config", sep = "/"), header = T, sep = " ")
    rank_data <- read_histogramlist(list.files(path = paste(exp, "rank", sep = "/"), pattern = "txt$", full.names = T), config$prefill[1], config$dist[1])
    rank_list[[i]] <- rank_data
    delay_data <- read_histogramlist(list.files(path = paste(exp, "delay", sep = "/"), pattern = "txt$", full.names = T), config$prefill[1], config$dist[1])
    delay_list[[i]] <- delay_data
    top_delay_data <- read_histogramlist(list.files(path = paste(exp, "top_delay", sep = "/"), pattern = "txt$", full.names = T), config$prefill[1], config$dist[1])
    top_delay_list[[i]] <- top_delay_data
    throughput_data <- read_throughputlist(list.files(path = paste(exp, "throughput", sep = "/"), pattern = "txt$", full.names = T), config$prefill[1], config$dist[1])
    throughput_list[[i]] <- throughput_data
    i <- i + 1
  }
  rank_data <- do.call(rbind, rank_list)
  delay_data <- do.call(rbind, delay_list)
  top_delay_data <- do.call(rbind, top_delay_list)
  throughput_data <- do.call(rbind, throughput_list)
  print(throughput_data)
  list("rank" = rank_data, "delay" = delay_data, "top_delay" = top_delay_data, "throughput" = throughput_data)
}

data <- read_scenario("prefill")
plot_scenario(data, paste(experiment_dir, "prefill", sep = "/"))
plot_throughput_by_prefill(data$throughput, paste(experiment_dir, "prefill", sep = "/"))

data <- read_scenario("distribution")
plot_scenario(data, paste(experiment_dir, "distribution", sep = "/"))
plot_throughput_by_dist(data$throughput, paste(experiment_dir, "distribution", sep = "/"))
