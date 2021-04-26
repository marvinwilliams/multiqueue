library(ggplot2)
library(tidyverse)
library(dplyr)
library(reshape2)
library(stringr)

experiment_dir <- "./experiments"

read_histogram <- function(file, name, threads, prefill, dist, sleep) {
  if (file.exists(file)) {
    data <- read.csv(file = file, header = F, col.names = c("rank", "frequency"), sep = " ")
    data$name <- name
    data["cummulated"] <- rev(cumsum(rev(data$frequency)))
    data$cummulated <- data$cummulated / data$cummulated[1]
    data$threads <- threads
    data$prefill <- prefill
    data$dist <- dist
    data$sleep <- sleep
    data
  } else {
    data <- data.frame(matrix(NA, ncol = 8, nrow = 1))
    colnames(data) <- c("rank", "frequency", "name", "cummulated", "threads", "prefill", "dist", "sleep")
    data
  }
}

read_throughput <- function(file, name, prefill, dist) {
  if (file.exists(file)) {
    data <- read.csv(file = file, header = T, sep = " ")
    data <- data %>%
      group_by(threads) %>%
      summarize(mean = mean(throughput / 1000000), sd = sd(throughput / 1000000))
    data$name <- name
    data$prefill <- prefill
    data$dist <- dist
    data.frame(data)
  } else {
    data <- data.frame(matrix(NA, ncol = 6, nrow = 1))
    colnames(data) <- c("threads", "mean", "sd", "name", "prefill", "dist")
    data
  }
}


plot_histogram <- function(data, names, outdir, plotname, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cummulated, group = name, color = name)) +
        labs(x = "Rank", y = "Cummul. Frequency", title = title) +
        geom_line() +
        scale_color_brewer(palette = "Set1") +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/", title, "_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

plot_histogram_by_stickiness <- function(data, outdir, title) {
  plot_data <- data %>% filter(str_detect(name, "(fullbuffering|merging)mq_c_\\d+_k_\\d+_ibs_16_dbs_16_numa") & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data$c <- as.factor(str_extract(plot_data$name, "(?<=_c_)\\d+"))
  c_order <- paste(sort(unique(as.integer(levels(plot_data$c)))))
  plot_data$c <- factor(plot_data$c, levels = c_order)
  plot_data$k <- as.factor(str_extract(plot_data$name, "(?<=_k_)\\d+"))
  k_order <- paste(sort(unique(as.integer(levels(plot_data$k)))))
  plot_data$k <- factor(plot_data$k, levels = k_order)
  plot_data$type <- str_extract(plot_data$name, "(fullbuffering|merging)mq")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cummulated, group = interaction(type, c, k), color = interaction(type, k))) +
        labs(x = title, y = "Cummul. Frequency", title = title) +
        geom_line() +
        scale_color_brewer(palette = "Set1") +
        facet_wrap(~c, ncol = 1) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/", title, "_by_stickiness_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

plot_histogram_by_prefill <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & dist == "uniform" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cummulated, group = interaction(name, prefill), color = name)) +
        labs(x = title, y = "Cummul. Frequency", title = title) +
        geom_line() +
        scale_color_brewer(palette = "Set1") +
        facet_wrap(~prefill, ncol = 1) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/", title, "_by_prefill_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

plot_histogram_by_dist <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cummulated, group = interaction(name, dist), color = name)) +
        labs(x = title, y = "Cummul. Frequency", title = title) +
        geom_line() +
        scale_color_brewer(palette = "Set1") +
        facet_wrap(~dist, ncol = 1) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/", title, "_by_dist_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

plot_histogram_by_sleep <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cummulated, group = interaction(name, sleep), color = name)) +
        labs(x = title, y = "Cummul. Frequency", title = title) +
        geom_line() +
        scale_color_brewer(palette = "Set1") +
        facet_wrap(~sleep, ncol = 1) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/", title, "_by_sleep_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

# plot_histogram_bar <- function(data, outdir, plotname) {
#   plot_data <- data[data$name %in% c("nobufferingmq", "fullbufferingmq", "insertionbuffermq", "deletionbuffermq", "mergingmq", "numamq", "numamergingmq", "wrapper_linden", "wrapper_capq", "wrapper_dlsm", "wrapper_spraylist"), ] %>%
#   group_by(threads, name) %>%
#     summarize(mean = weighted.mean(rank, frequency), max = max(rank), .groups = "keep") %>%
#     do ({
#       print(.)
#   transformed = melt(., measure.vars = c("mean", "max"))
#   plot <- ggplot(transformed, aes(x = as.factor(name), y = value, fill = name)) +
#     labs(x = "Priority Queue", y = "Top Delay", title = "Top Delay") +
#     geom_bar(stat = "identity") +
#     scale_color_brewer(palette = "Set1") +
#     # facet_grid(cols = vars(variable), rows = vars(prefill, dist), scales = "free") +
#     facet_grid(rows = vars(variable), scales = "free") +
#     theme(axis.ticks.x = element_blank(), axis.text.x = element_blank())
#   ggsave(plot, file = paste(outdir, "/", plotname, "_", .$threads[1], ".pdf", sep = ""))
#   .
#   })
# }

plot_throughput_by_thread <- function(data, names, outdir) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot <- ggplot(plot_data, aes(x = threads, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    scale_color_brewer(palette = "Set1") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput.pdf", sep = ""))
}

plot_throughput_by_buffer_size <- function(data, outdir) {
  plot_data <- data %>% filter(str_detect(name, "fullbufferingmq_c_4_k_1_ibs_\\d+_dbs_\\d+_numa") & prefill == "1000000" & dist == "uniform")

  plot_data$ibs <- as.factor(str_extract(plot_data$name, "(?<=_ibs_)\\d+"))
  ibs_order <- paste(sort(unique(as.integer(levels(plot_data$ibs)))))
  plot_data$ibs <- factor(plot_data$ibs, levels = ibs_order)

  plot_data$dbs <- as.factor(str_extract(plot_data$name, "(?<=_dbs_)\\d+"))
  dbs_order <- paste(sort(unique(as.integer(levels(plot_data$dbs)))))
  plot_data$dbs <- factor(plot_data$dbs, levels = dbs_order)

  plot <- ggplot(plot_data, aes(x = threads, y = mean, group = interaction(ibs, dbs), color = dbs)) +
    geom_line() +
    geom_point() +
    # scale_shape_manual(values = rep(1:5, length.out = 24)) +
    scale_color_brewer(palette = "Set1") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~ibs, ncol = 1) +
    labs(x = "p", y = "10^6 Ops/s", color = "Deletion buffer size", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_buffer_size.pdf", sep = ""))
}

plot_throughput_by_stickiness <- function(data, outdir) {
  plot_data <- data %>% filter(str_detect(name, "(fullbuffering|merging)mq_c_\\d+_k_\\d+_ibs_16_dbs_16_numa") & prefill == "1000000" & dist == "uniform")

  plot_data$c <- as.factor(str_extract(plot_data$name, "(?<=_c_)\\d+"))
  c_order <- paste(sort(unique(as.integer(levels(plot_data$c)))))
  plot_data$c <- factor(plot_data$c, levels = c_order)

  plot_data$k <- as.factor(str_extract(plot_data$name, "(?<=_k_)\\d+"))
  k_order <- paste(sort(unique(as.integer(levels(plot_data$k)))))
  plot_data$k <- factor(plot_data$k, levels = k_order)

  plot_data$type <- str_extract(plot_data$name, "(fullbuffering|merging)mq")
  plot <- ggplot(plot_data, aes(x = threads, y = mean, group = interaction(type, c, k), color = k)) +
    geom_line() +
    geom_point() +
    # scale_shape_manual(values = rep(1:5, length.out = 24)) +
    scale_color_brewer(palette = "Set1") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~c, ncol = 1) +
    labs(x = "p", y = "10^6 Ops/s", color = "Stickiness", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput_by_stickiness.pdf", sep = ""))
}

plot_throughput_by_ns <- function(data, outdir) {
  plot_data <- data %>% filter(str_detect(name, "mergingmq_c_4_k_1_ns_\\d+_numa") & prefill == "1000000" & dist == "uniform")

  plot_data$ns <- as.factor(str_extract(plot_data$name, "(?<=_ns_)\\d+"))
  ns_order <- paste(sort(unique(as.integer(levels(plot_data$ns)))))
  plot_data$ns <- factor(plot_data$ns, levels = ns_order)

  plot <- ggplot(plot_data, aes(x = threads, y = mean, group = ns, color = ns)) +
    geom_line() +
    geom_point() +
    # scale_shape_manual(values = rep(1:5, length.out = 24)) +
    scale_color_brewer(palette = "Set1") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second", color = "Node size")
  ggsave(plot, file = paste(outdir, "/throughput_by_ns.pdf", sep = ""))
}

plot_sssp <- function(data, names, outdir) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot <- ggplot(plot_data, aes(x = threads, y = mean, group = name, color = name)) +
    geom_line() +
    geom_point() +
    scale_color_brewer(palette = "Set1") +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = "10^6 Ops/s", title = "Operations per second")
  ggsave(plot, file = paste(outdir, "/throughput.pdf", sep = ""))
}
plot_scenario <- function(data, dir) {
  pq_names <- c("wrapper_capq", "wrapper_klsm256", "wrapper_klsm1024", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_64_ibs_16_dbs_16_numa", "mergingmq_c_4_k_8_ns_128_numa")
  variant_names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128", "mergingmq_c_4_k_8_ns_128_numa", "wrapper_capq", "wrapper_klsm256", "wrapper_klsm1024")
  # plot_histogram(data$rank, pq_names, dir, "rank")
  # plot_histogram_by_stickiness(data$rank, dir, "rank")
  # plot_histogram(data$delay, pq_names, dir, "delay")
  # plot_histogram_by_stickiness(data$delay, dir, "delay")
  plot_histogram_by_prefill(data$rank, variant_names, dir, "rank")
  plot_histogram_by_dist(data$rank, variant_names, dir, "rank")
  plot_histogram_by_sleep(data$rank, variant_names, dir, "rank")
  # plot_histogram_bar(data$top_delay, dir, "top_delay")
  # plot_throughput_by_thread(data$throughput, pq_names, dir)
  # plot_throughput_by_buffer_size(data$throughput, dir)
  # plot_throughput_by_stickiness(data$throughput, dir)
  # plot_throughput_by_ns(data$throughput, dir)
}

read_scenario <- function(scenario) {
  scenario_data <- list()
  rank_list <- list()
  delay_list <- list()
  top_delay_list <- list()
  throughput_list <- list()
  hist_index <- 1
  throughput_index <- 1
  for (pq in list.dirs(path = paste(experiment_dir, scenario, sep = "/"), full.names = T, recursive = F)) {
    name <- gsub(".*/", "", pq)
    for (j in c("1", "2", "4", "8", "16", "32", "64", "128")) {
      rank_data <- read_histogram(paste(pq, "/rank_", j, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", "0")
      rank_list[[hist_index]] <- rank_data

      delay_data <- read_histogram(paste(pq, "/delay_", j, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", "0")
      delay_list[[hist_index]] <- delay_data

      top_delay_data <- read_histogram(paste(pq, "/top_delay_", j, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", "0")
      top_delay_list[[hist_index]] <- top_delay_data

      hist_index <- hist_index + 1

      for (prefill in c("0", "1000", "10000000")) {
        rank_data <- read_histogram(paste(pq, "/rank_", j, "_n_", prefill, ".txt", sep = ""), name, as.integer(j), prefill, "uniform", "0")
        rank_list[[hist_index]] <- rank_data

        delay_data <- read_histogram(paste(pq, "/delay_", j, "_n_", prefill, ".txt", sep = ""), name, as.integer(j), prefill, "uniform", "0")
        delay_list[[hist_index]] <- delay_data

        top_delay_data <- read_histogram(paste(pq, "/top_delay_", j, "_n_", prefill, ".txt", sep = ""), name, as.integer(j), prefill, "uniform", "0")
        top_delay_list[[hist_index]] <- top_delay_data

        hist_index <- hist_index + 1
      }

      for (dist in c("ascending", "descending", "threadid")) {
        rank_data <- read_histogram(paste(pq, "/rank_", j, "_dist_", dist, ".txt", sep = ""), name, as.integer(j), "1000000", dist, "0")
        rank_list[[hist_index]] <- rank_data

        delay_data <- read_histogram(paste(pq, "/delay_", j, "_dist_", dist, ".txt", sep = ""), name, as.integer(j), "1000000", prefill, "0")
        delay_list[[hist_index]] <- delay_data

        top_delay_data <- read_histogram(paste(pq, "/top_delay_", j, "_dist_", dist, ".txt", sep = ""), name, as.integer(j), "1000000", prefill, "0")
        top_delay_list[[hist_index]] <- top_delay_data

        hist_index <- hist_index + 1
      }

      for (sleep in c("1000", "5000000")) {
        rank_data <- read_histogram(paste(pq, "/rank_", j, "_s_", sleep, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", sleep)
        rank_list[[hist_index]] <- rank_data

        delay_data <- read_histogram(paste(pq, "/delay_", j, "_s_", sleep, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", sleep)
        delay_list[[hist_index]] <- delay_data

        top_delay_data <- read_histogram(paste(pq, "/top_delay_", j, "_s_", sleep, ".txt", sep = ""), name, as.integer(j), "1000000", "uniform", sleep)
        top_delay_list[[hist_index]] <- top_delay_data

        hist_index <- hist_index + 1
      }
    }
    throughput_data <- read_throughput(paste(pq, "/throughput.txt", sep = ""), name, "1000000", "uniform")
    throughput_list[[throughput_index]] <- throughput_data
    throughput_index <- throughput_index + 1
    for (prefill in c("0", "1000", "10000000")) {
      throughput_data <- read_throughput(paste(pq, "/throughput_n_", prefill, ".txt", sep = ""), name, prefill, "uniform")
      throughput_list[[throughput_index]] <- throughput_data
      throughput_index <- throughput_index + 1
    }
    for (dist in c("ascending", "descending", "threadid")) {
      throughput_data <- read_throughput(paste(pq, "/throughput_dist_", dist, ".txt", sep = ""), name, "1000000", dist)
      throughput_list[[throughput_index]] <- throughput_data
      throughput_index <- throughput_index + 1
    }
  }
  rank_data <- do.call(rbind, rank_list) %>% drop_na()
  delay_data <- do.call(rbind, delay_list) %>% drop_na()
  top_delay_data <- do.call(rbind, top_delay_list) %>% drop_na()
  throughput_data <- do.call(rbind, throughput_list) %>% drop_na()
  list("rank" = rank_data, "delay" = delay_data, "top_delay" = top_delay_data, "throughput" = throughput_data)
}

data <- read_scenario("i10pc136/results")
plot_scenario(data, paste(experiment_dir, "i10pc136", sep = "/"))
