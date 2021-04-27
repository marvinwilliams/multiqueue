library(ggplot2)
library(tidyverse)
library(dplyr)
library(reshape2)
library(stringr)
library(scales)

read_histogram <- function(file, name, threads, prefill, dist, sleep) {
  if (file.exists(file)) {
    data <- read.csv(file = file, header = F, col.names = c("rank", "frequency"), sep = " ")
    data$name <- name
    data["cumulated"] <- rev(cumsum(rev(data$frequency)))
    data$cumulated <- data$cumulated / data$cumulated[1]
    data$threads <- threads
    data$prefill <- prefill
    data$dist <- dist
    data$sleep <- sleep
    # write.table(data, file = "frequency.txt")
    # print(sum(data$frequency))
    data
  } else {
    data <- data.frame(matrix(NA, ncol = 8, nrow = 1))
    colnames(data) <- c("rank", "frequency", "name", "cumulated", "threads", "prefill", "dist", "sleep")
    data
  }
}

read_throughput <- function(file, name, prefill, dist) {
  if (file.exists(file)) {
    data <- read.csv(file = file, header = T, sep = " ")
    data <- data %>%
      group_by(threads) %>%
      summarize(mean = mean(throughput / 1000000), sd = sd(throughput / 1000000))
    data$name <- factor(name)
    data$prefill <- prefill
    data$dist <- dist
    data.frame(data)
  } else {
    data <- data.frame(matrix(NA, ncol = 6, nrow = 1))
    colnames(data) <- c("threads", "mean", "sd", "name", "prefill", "dist")
    data
  }
}

read_sssp <- function(file, name, graph) {
  if (file.exists(file)) {
    data <- read.csv(file = file, header = F, sep = " ")
    colnames(data) <- c("threads", "time", "relaxed")
    data$name <- name
    data$graph <- graph
    data
  } else {
    data <- data.frame(matrix(NA, ncol = 5, nrow = 1))
    colnames(data) <- c("threads", "time", "relaxed", "name", "graph")
    data
  }
}

plot_histogram_theory <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = interaction(name, sleep), linetype = name)) +
        geom_line() +
        stat_function(fun = function(x) (1 - 2 / (2 * .$threads[1]))^(x - 1), aes(color = "theo 2", linetype = "theo 2")) +
        stat_function(fun = function(x) (1 - 2 / (4 * .$threads[1]))^(x - 1), aes(color = "theo 4", linetype = "theo 4")) +
        stat_function(fun = function(x) (1 - 2 / (8 * .$threads[1]))^(x - 1), aes(color = "theo 8", linetype = "theo 8")) +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(values = c(2, 3, 4, 2, 3, 4), labels = c("MultiQueue C = 2", "MultiQueue C = 4", "MultiQueue C = 8", "Theoretical C = 2", "Theoretical C = 4", "Theoretical C = 8")) +
        scale_linetype_manual(values = c(1, 1, 1, 2, 2, 2), labels = c("MultiQueue C = 2", "MultiQueue C = 4", "MultiQueue C = 8", "Theoretical C = 2", "Theoretical C = 4", "Theoretical C = 8")) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
      ggsave(plot, file = paste(outdir, "/", tolower(title), "_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      plot
    })
}

p_labeller <- function(variable, value) {
  paste("p =", value)
}

plot_histogram_theory_wrapped <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "100000" & threads %in% c(2, 8, 32, 64))
  theory <- plot_data %>%
    mutate(cumulated_2 = (1 - 2 / (2 * threads))^(rank), cumulated_4 = (1 - 2 / (4 * threads))^(rank ), cumulated_8 = (1 - 2 / (8 * threads))^(rank))

  plot <- ggplot(plot_data, aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
    geom_line() +
    geom_line(aes(y = cumulated_2, color = "theo 2", linetype = "theo 2"), data = theory) +
    geom_line(aes(y = cumulated_4, color = "theo 4", linetype = "theo 4"), data = theory) +
    geom_line(aes(y = cumulated_8, color = "theo 8", linetype = "theo 8"), data = theory) +
    scale_x_log10() +
    coord_cartesian(xlim = c(1, 20000)) +
    labs(x = "Rank", y = "Cumulative Frequency") +
    scale_color_manual(values = c(2, 3, 4, 2, 3, 4), labels = c("MultiQueue C = 2", "MultiQueue C = 4", "MultiQueue C = 8", "Theoretical C = 2", "Theoretical C = 4", "Theoretical C = 8")) +
    scale_linetype_manual(values = c(1, 1, 1, 2, 2, 2), labels = c("MultiQueue C = 2", "MultiQueue C = 4", "MultiQueue C = 8", "Theoretical C = 2", "Theoretical C = 4", "Theoretical C = 8")) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
    facet_wrap(~threads, ncol = 1, labeller = p_labeller) + 
    theme_bw()
    ggsave(plot, file = paste(outdir, "/", tolower(title), ".pdf", sep = ""), width = 6, height = 5)
}

plot_rank <- function(data, names, outdir) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0" )
  # plot_data$name <- factor(plot_data$name, levels = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa", "theo 4"))
  theory <- plot_data %>%
    mutate(cumulated_4 = (1 - 2 / (4 * threads))^(rank ))
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        geom_line(aes(y = cumulated_4, color = "theo 4", linetype = "theo 4"), data = theory[theory$threads == .$threads[1],]) +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        # scale_color_manual(breaks = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa", "theo 4"), labels = c("no buffering", "b = 4", "b = 8", "b = 16",  "b = 64", "Theoretical C = 4"), values = c(2, 3, 4, 6, 7, 8)) +
        # scale_linetype_manual(breaks = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa", "theo 4"), labels = c("no buffering", "b = 4", "b = 8", "b = 16",  "b = 64", "Theoretical C = 4"), values = c(1, 1, 1, 1, 1, 8)) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() + 
        theme(legend.position = c(0.85, 0.80))
        ggsave(plot, file = paste(outdir, "/", "rank_" , .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
  return()
}

plot_delay <- function(data, names, outdir) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0" & threads %in% c(16, 64, 128))
  # plot_data$name <- factor(plot_data$name, levels = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa", "theo 4"))
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Delay", y = "Cumulative Frequency") +
        scale_color_manual(breaks = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa"), labels = c("no buffering", "b = 4", "b = 8", "b = 16",  "b = 64"), values = c(2, 3, 4, 6, 7)) +
        guides(color = guide_legend(title = NULL)) +
        theme_bw() + 
        theme(legend.position = c(0.85, 0.80))
      ggsave(plot, file = paste(outdir, "/", "delay_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
  return()
}

c_labeller <- function(variable, value) {
  paste("C =", value)
}

plot_histogram_by_buffer_size <- function(data, outdir, title) {
  plot_data <- data %>% filter((name == "nobufferingmq_numa" | str_detect(name, "fullbufferingmq_c_4_k_1_ibs_\\d+_dbs_\\d+_numa")) & prefill == "1000000" & dist == "uniform" & sleep == "0")

  plot_data$ibs <- as.factor(str_extract(plot_data$name, "(?<=_ibs_)\\d+"))
  ibs_order <- paste(sort(unique(as.integer(levels(plot_data$ibs)))))
  plot_data$ibs <- factor(plot_data$ibs, levels = ibs_order)

  plot_data$dbs <- as.factor(str_extract(plot_data$name, "(?<=_dbs_)\\d+"))
  dbs_order <- paste(sort(unique(as.integer(levels(plot_data$dbs)))))
  plot_data$dbs <- factor(plot_data$dbs, levels = dbs_order)

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cumulated, group = dbs, color = dbs)) +
        labs(x = title, y = "Cumulative Frequency", title = title) +
        geom_line() +
        scale_x_log10()
      save_plot(plot, paste(outdir, "/", tolower(title), "_by_buffer_size_", .$threads[1], ".pdf", sep = ""))
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
      plot <- ggplot(., aes(x = rank, y = cumulated, group = interaction(type, c, k), color = interaction(type, k))) +
        labs(x = title, y = "Cumulative Frequency", title = title) +
        geom_line() +
        facet_wrap(~c, ncol = 1, labeller = c_labeller) +
        scale_x_log10()
      save_plot(plot, paste(outdir, "/", tolower(title), "_by_stickiness_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

prefill_labeller <- function(variable, value) {
  paste("n =", value)
}

plot_histogram_by_prefill <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & dist == "uniform" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cumulated, group = interaction(name, prefill), color = name)) +
        labs(x = title, y = "Cumulative Frequency", title = title) +
        geom_line() +
        facet_wrap(~prefill, ncol = 1, labeller = prefill_labeller) +
        scale_x_log10()
      save_plot(plot, paste(outdir, "/", tolower(title), "_by_prefill_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

dist_labeller <- function(variable, value) {
  paste("Key distribution:", value)
}

plot_histogram_by_dist <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cumulated, group = interaction(name, dist), color = name)) +
        labs(x = title, y = "Cumulative Frequency", title = title) +
        geom_line() +
        facet_wrap(~dist, ncol = 1, labeller = dist_labeller) +
        scale_x_log10()
      save_plot(plot, paste(outdir, "/", tolower(title), "_by_dist_", .$threads[1], ".pdf", sep = ""))
      .
    })
}

sleep_labeller <- function(variable, value) {
  paste("s =", as.numeric(value) / 1000, "us")
}

plot_histogram_by_sleep <- function(data, names, outdir, title) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank, y = cumulated, group = interaction(name, sleep), color = name)) +
        labs(x = title, y = "Cumulative Frequency", title = title) +
        geom_line() +
        facet_wrap(~sleep, ncol = 1, labeller = sleep_labeller) +
        scale_x_log10()
      save_plot(plot, paste(outdir, "/", tolower(title), "_by_sleep_", .$threads[1], ".pdf", sep = ""))
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
#     # facet_grid(cols = vars(variable), rows = vars(prefill, dist), scales = "free") +
#     facet_grid(rows = vars(variable), scales = "free") +
#     theme(axis.ticks.x = element_blank(), axis.text.x = element_blank())
#   save_plot(plot,  paste(outdir, "/", plotname, "_", .$threads[1], ".pdf", sep = ""))
#   .
#   })
# }

plot_throughput_by_thread <- function(data, names, outdir) {
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data$name <- factor(plot_data$name, levels = c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa"))
  plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = "10^6 Ops/s") +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks =c(1, 2, 4, 8, 16, 32, 64, 80) , minor_breaks = NULL, limits=c(1, 80), oob = squish) +
    scale_color_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
    guides(color = guide_legend(title = NULL)) +
    theme_bw() +
    theme(legend.position = c(0.15, 0.82))
    ggsave(plot, file = paste(outdir, "/throughput.pdf", sep = ""), width = 6, height = 5)
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
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    scale_x_continuous("p", labels = as.character(plot_data$threads), breaks = plot_data$threads, minor_breaks = NULL) +
    labs(x = "p", y = "10^6 Ops/s", color = "Buffer size", title = "Throughput")
  save_plot(plot, paste(outdir, "/throughput_by_buffer_size.pdf", sep = ""))
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
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    facet_wrap(~c, ncol = 1, labeller = c_labeller) +
    scale_x_continuous("p", labels = as.character(plot_data$threads), breaks = plot_data$threads, minor_breaks = NULL) +
    labs(x = "p", y = "10^6 Ops/s", color = "Stickiness", title = "Throughput")
  save_plot(plot, paste(outdir, "/throughput_by_stickiness.pdf", sep = ""))
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
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = "10^6 Ops/s", title = "Throughput", color = "Node size")
  save_plot(plot, paste(outdir, "/throughput_by_ns.pdf", sep = ""))
}

plot_sssp <- function(data, outdir, graph_name) {
  plot_data <- data %>% filter(graph == graph_name)
  plot <- ggplot(plot_data, aes(x = threads, y = time, group = name, color = name)) +
    geom_line() +
    geom_point() +
    labs(x = "p", y = "Time [ms]", title = "Time")
  save_plot(plot, paste(outdir, "/sssp_time_", graph_name, ".pdf", sep = ""))
  plot <- ggplot(plot_data, aes(x = threads, y = relaxed, group = name, color = name)) +
    geom_line() +
    geom_point() +
    labs(x = "p", y = "# Relaxed nodes", title = "Relaxed Nodes")
  save_plot(plot, paste(outdir, "/sssp_nodes_", graph_name, ".pdf", sep = ""))
}

read_scenario <- function(scenario) {
  scenario_data <- list()
  rank_list <- list()
  delay_list <- list()
  top_delay_list <- list()
  throughput_list <- list()
  sssp_list <- list()
  hist_index <- 1
  sssp_index <- 1
  throughput_index <- 1
  for (pq in list.dirs(path = scenario, full.names = T, recursive = F)) {
    name <- gsub(".*/", "", pq)
    for (j in c("1", "2", "4", "8", "16", "32", "56", "64", "128")) {
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

      for (sleep in c("100000")) {
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
    for (graph in c("NY", "USA", "CTR", "CAL", "GER", "rhg_20", "rhg_22", "rhg_24")) {
      sssp_data <- read_sssp(paste(pq, "/sssp_", graph, ".txt", sep = ""), name, graph)
      sssp_list[[sssp_index]] <- sssp_data
      sssp_index <- sssp_index + 1
    }
  }
  rank_data <- do.call(rbind, rank_list) %>% drop_na()
  delay_data <- do.call(rbind, delay_list) %>% drop_na()
  top_delay_data <- do.call(rbind, top_delay_list) %>% drop_na()
  throughput_data <- do.call(rbind, throughput_list) %>% drop_na()
  sssp_data <- do.call(rbind, sssp_list) %>% drop_na()
  list("rank" = rank_data, "delay" = delay_data, "top_delay" = top_delay_data, "throughput" = throughput_data, "sssp" = sssp_data)
}

plot_scenario <- function(data, dir) {
  pq_names <- c("wrapper_capq", "wrapper_klsm256", "wrapper_klsm1024", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_64_ibs_16_dbs_16_numa", "mergingmq_c_4_k_8_ns_128_numa")
  variant_names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128", "mergingmq_c_4_k_8_ns_128_numa", "wrapper_capq", "wrapper_klsm256", "wrapper_klsm1024")

  # plot_histogram_theory_wrapped(data$rank, c("nobufferingmq_c_2_k_1_numa", "nobufferingmq_c_4_k_1_numa", "nobufferingmq_c_8_k_1_numa"), dir, "rank_theory")
  # plot_delay(data$delay, c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa"), dir)
  # plot_histogram_theory(data$rank, c("fullbufferingmq_c_2_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_1_ibs_16_dbs_16_numa"), dir, "rank_theory")
  plot_rank(data$rank, c("old","new"), dir)
  # plot_histogram_by_stickiness(data$rank, dir, "Rank")
  # plot_histogram_by_buffer_size(data$rank, dir, "Rank")
  # plot_histogram(data$delay, pq_names, dir, "Delay")
  # plot_histogram_by_stickiness(data$delay, dir, "Delay")
  # plot_histogram_by_prefill(data$rank, variant_names, dir, "Rank")
  # plot_histogram_by_dist(data$rank, variant_names, dir, "Rank")
  # plot_histogram_by_sleep(data$rank, variant_names, dir, "Rank")

  # plot_histogram_bar(data$top_delay, dir, "top_delay")

  # plot_throughput_by_thread(data$throughput, c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa"), dir)
  # plot_throughput_by_buffer_size(data$throughput, dir)
  # plot_throughput_by_stickiness(data$throughput, dir)
  # plot_throughput_by_ns(data$throughput, dir)
  # plot_sssp(data$sssp, dir, "NY")
  # plot_sssp(data$sssp, dir, "CAL")
  # plot_sssp(data$sssp, dir, "CTR")
  # plot_sssp(data$sssp, dir, "USA")
  # plot_sssp(data$sssp, dir, "GER")
  # plot_sssp(data$sssp, dir, "rhg_20")
  # plot_sssp(data$sssp, dir, "rhg_22")
  # plot_sssp(data$sssp, dir, "rhg_24")
}


experiment_dir <- "./experiments"
# read_histogram("rank_histogram56.txt", "", "", "", "", "")
args <- commandArgs(trailingOnly = T)
path <- paste(experiment_dir, args[1], sep = "/")
print(paste("Reading results for", path))
if (dir.exists(path)) {
  data <- read_scenario(paste(path, "results", sep = "/"))
  print("done")
  plot_scenario(data, path)
} else {
  print("Result directory not found")
}