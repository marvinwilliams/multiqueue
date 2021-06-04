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

plot_rank_histogram_theory <- function(data, outdir) {
  names <- c("nobufferingmq_c_2_k_1_numa", "nobufferingmq_c_4_k_1_numa", "nobufferingmq_c_8_k_1_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
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
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.85, 0.80))
      ggsave(plot, file = paste(outdir, "/", "rank_theory_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

p_labeller <- function(variable, value) {
  paste("p =", value)
}

plot_rank_histogram_theory_wrapped <- function(data, outdir) {
  names <- c("nobufferingmq_c_2_k_1_numa", "nobufferingmq_c_4_k_1_numa", "nobufferingmq_c_8_k_1_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0" & threads %in% c(8, 64))
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
    scale_color_manual(breaks = c("nobufferingmq_c_2_k_1_numa", "theo 2", "nobufferingmq_c_4_k_1_numa", "theo 4", "nobufferingmq_c_8_k_1_numa", "theo 8"), values = c(2, 2, 3, 3, 4, 4), labels = c("MultiQueue C = 2", "Theoretical C = 2", "MultiQueue C = 4", "Theoretical C = 4", "MultiQueue C = 8", "Theoretical C = 8")) +
    scale_linetype_manual(breaks = c("nobufferingmq_c_2_k_1_numa", "theo 2", "nobufferingmq_c_4_k_1_numa", "theo 4", "nobufferingmq_c_8_k_1_numa", "theo 8"), values = c(1, 2, 1, 2, 1, 2), labels = c("MultiQueue C = 2", "Theoretical C = 2", "MultiQueue C = 4", "Theoretical C = 4", "MultiQueue C = 8", "Theoretical C = 8")) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
    facet_wrap(~threads, nrow = 1, labeller = p_labeller) + 
    theme_bw() +
    theme(legend.position = "top", panel.grid.major = element_blank(), panel.grid.minor = element_blank(), strip.background = element_blank())
    ggsave(plot, file = paste(outdir, "/rank_theory.pdf", sep = ""), width = 5.5, height = 3)
}

 plot_throughput_buffer_size_c_2 <- function(data, outdir) {
   names <- c("nobufferingmq_c_2_k_1_numa", "intmq_c_2_k_1_ibs_4_dbs_4_numa", "intmq_c_2_k_1_ibs_8_dbs_8_numa", "intmq_c_2_k_1_ibs_16_dbs_16_numa",  "intmq_c_2_k_1_ibs_64_dbs_64_numa")
   plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
   plot_data$name <- factor(plot_data$name, levels = names)
   plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name, shape = name)) +
     geom_line() +
     geom_point() +
     geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
       width = .2,
       position = position_dodge(0.05)
     ) +
     labs(x = "p", y = bquote(10^6 ~ "Ops/s")) +
     scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
     scale_y_continuous(labels = c("0", "100", "200", "300"), breaks = c(0, 100, 200, 300), limits=c(0, 330)) +
     scale_color_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
     scale_shape_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
     guides(color = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
     theme_bw() +
    theme(legend.position = c(0.25, 0.65), panel.grid.major.x = element_blank())
     ggsave(plot, file = paste(outdir, "/throughput_buffer_size_c_2.pdf", sep = ""), width = 3.5, height = 2.7)
 }

plot_throughput_buffer_size_c_4 <- function(data, outdir) {
  names <- c("nobufferingmq_c_4_k_1_numa", "intmq_c_4_k_1_ibs_4_dbs_4_numa", "intmq_c_4_k_1_ibs_8_dbs_8_numa", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_1_ibs_64_dbs_64_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data$name <- factor(plot_data$name, levels = names)
  plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name, shape = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = bquote(10^6 ~ "Ops/s")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
     scale_y_continuous(labels = c("0", "100", "200", "300"), breaks = c(0, 100, 200, 300), limits=c(0, 330)) +
    scale_color_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
    scale_shape_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
    guides(color = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(legend.position = "none", panel.grid.major.x = element_blank())
    ggsave(plot, file = paste(outdir, "/throughput_buffer_size_c_4.pdf", sep = ""), width = 3.5, height = 2.7)
}

plot_rank_buffer_size <- function(data, outdir) {
  names <- c("nobufferingmq_c_4_k_1_numa", "intmq_c_4_k_1_ibs_4_dbs_4_numa",  "intmq_c_4_k_1_ibs_64_dbs_64_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        stat_function(fun = function(x) (1 - 2 / (4 * .$threads[1]))^(x - 1), aes(color = "theo 4", linetype = "theo 4")) +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(values = c(3, 2, 4, 3), labels = c("no buffering", "b = 4", "b = 64", "Theoretical")) +
        scale_linetype_manual(values = c(1, 1, 1, 2), labels = c("no buffering", "b = 4", "b = 64", "Theoretical")) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.85, 0.85))
      ggsave(plot, file = paste(outdir, "/", "rank_buffer_size_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

plot_delay_buffer_size <- function(data, outdir) {
  names <- c("nobufferingmq_c_4_k_1_numa", "intmq_c_4_k_1_ibs_4_dbs_4_numa",  "intmq_c_4_k_1_ibs_64_dbs_64_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Delay", y = "Cumulative Frequency") +
        scale_color_manual(values = c(3, 2, 4, 3), labels = c("no buffering", "b = 4", "b = 64")) +
        scale_linetype_manual(values = c(1, 1, 1, 2), labels = c("no buffering", "b = 4", "b = 64")) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.85, 0.85))
      ggsave(plot, file = paste(outdir, "/", "delay_buffer_size_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

table_by_c_and_stickiness <- function(data, outdir) {
  rank_data <- data$rank %>% filter(str_detect(name, "intmq_c_\\d+_k_\\d+_ibs_16_dbs_16_numa") & prefill == "1000000" & dist == "uniform" & threads == 64 & sleep == "0") %>% group_by(name) %>% summarize(avg_rank = weighted.mean(rank, frequency))
  delay_data <- data$delay %>% filter(str_detect(name, "intmq_c_\\d+_k_\\d+_ibs_16_dbs_16_numa") & prefill == "1000000" & dist == "uniform" & threads == 64 & sleep == "0") %>% group_by(name) %>% summarize(avg_delay = weighted.mean(rank, frequency))
  throughput_data <- data$throughput %>% filter(str_detect(name, "intmq_c_\\d+_k_\\d+_ibs_16_dbs_16_numa") & prefill == "1000000" & dist == "uniform" & threads == 64)
  table_data <- inner_join(throughput_data, rank_data)
  table_data <- inner_join(table_data, delay_data)

  table_data$c <- as.factor(str_extract(table_data$name, "(?<=_c_)\\d+"))
  c_order <- paste(sort(unique(as.integer(levels(table_data$c)))))
  table_data$c <- factor(table_data$c, levels = c_order)

  table_data$s <- as.factor(str_extract(table_data$name, "(?<=_k_)\\d+"))
  s_order <- paste(sort(unique(as.integer(levels(table_data$s)))))
  table_data$s <- factor(table_data$s, levels = s_order)

  table_data <- table_data %>% select(c, s, mean, avg_rank, avg_delay);
  table <- xtable(table_data[with(table_data, order(c, s)),], digits = 1)

  print(table, file = paste(outdir, "/c_and_stickiness.tex", sep=''), include.rownames = F)
}

plot_throughput_merging <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa",
             "intmq_c_4_k_4_ibs_16_dbs_16_numa",
             "intmq_c_8_k_8_ibs_16_dbs_16_numa",
             "intmq_c_16_k_8_ibs_16_dbs_16_numa",
             "intmq_c_16_k_64_ibs_16_dbs_16_numa",
             "intmergingmq_c_2_k_1_ns_16_numa",
             "intmergingmq_c_4_k_4_ns_16_numa",
             "intmergingmq_c_8_k_8_ns_16_numa",
             "intmergingmq_c_16_k_8_ns_16_numa",
             "intmergingmq_c_16_k_64_ns_16_numa")
  labels <- c("8-ary heap, (2, 1)", "8-ary heap, (4, 4)", "8-ary heap, (8, 8)", "8-ary heap, (16, 8)",  "8-ary heap, (16, 64)", "merging heap, (2, 1)", "merging heap, (4, 1)", "merging heap, (8, 8)", "merging heap, (16, 8)",  "merging heap, (16, 64)")
  # names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_16_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_8_ns_128_numa", "mergingmq_c_8_k_8_ns_128_numa", "mergingmq_c_16_k_16_ns_128_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data$name <- factor(plot_data$name, levels = names)
  plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name, linetype = name, shape = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = bquote(10^6 ~ "Ops/s")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
    scale_color_manual(breaks = names, values = c(2, 3, 4, 6, 7, 2, 3, 4, 6, 7), labels = labels) +
    scale_linetype_manual(breaks = names, values = c(1, 1, 1, 1, 1, 3, 3, 3, 3, 3), labels = labels) +
    scale_shape_manual(breaks = names, values = 1:10, labels = labels) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(panel.grid.major.x = element_blank())
    ggsave(plot, file = paste(outdir, "/throughput_merging.pdf", sep = ""), width = 6, height = 4)
}

plot_rank_merging <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa",
             "intmq_c_4_k_1_ibs_16_dbs_16_numa",
             "intmq_c_4_k_4_ibs_16_dbs_16_numa",
             "intmq_c_8_k_8_ibs_16_dbs_16_numa",
             "intmq_c_16_k_8_ibs_16_dbs_16_numa",
             "intmergingmq_c_2_k_1_ns_16_numa",
             "intmergingmq_c_4_k_1_ns_16_numa",
             "intmergingmq_c_4_k_4_ns_16_numa",
             "intmergingmq_c_8_k_8_ns_16_numa",
             "intmergingmq_c_16_k_8_ns_16_numa")
  labels <- c("8-ary heap, (2, 1)", "8-ary heap, (4, 1)", "8-ary heap, (4, 4)", "8-ary heap, (8, 8)", "8-ary heap, (16, 8)",  "merging heap, (2, 1)", "merging heap, (4, 1)", "merging heap, (4, 4)", "merging heap, (8, 8)", "merging heap, (16, 8)")
  # names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_16_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_8_ns_128_numa", "mergingmq_c_8_k_8_ns_128_numa", "mergingmq_c_16_k_16_ns_128_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
    scale_color_manual(breaks = names, values = c(2, 3, 4, 6, 7, 2, 3, 4, 6, 7), labels = labels) +
    scale_linetype_manual(breaks = names, values = c(1, 1, 1, 1, 1, 3, 3, 3, 3, 3), labels = labels) +
    scale_shape_manual(breaks = names, values = 1:10, labels = labels) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.85, 0.85))
      ggsave(plot, file = paste(outdir, "/", "rank_merging_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

plot_rank_comparison <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa",   "intmq_c_8_k_8_ibs_16_dbs_16_numa", "wrapper_linden", "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq")
  labels <- c("MultiQueue (2, 1)", "MultiQueue (4, 1)",   "MultiQueue (4, 4)",    "MultiQueue (8, 8)", "linden", "spraylist", "klsm256", "klsm1024", "capq")
  # names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_16_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_8_ns_128_numa", "mergingmq_c_8_k_8_ns_128_numa", "mergingmq_c_16_k_16_ns_128_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(breaks = names, values = c(3, 3, 3, 3, 2, 4, 6, 6, 7), labels = labels) +
        scale_linetype_manual(breaks = names, values = c(1, 2, 3, 4, 1, 1, 1, 2, 1), labels = labels) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = "none")
      ggsave(plot, file = paste(outdir, "/", "rank_compare_", .$threads[1], ".pdf", sep = ""), width = 4, height = 2.5)
      .
    })
}

plot_delay_comparison <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa",
             "intmq_c_4_k_1_ibs_16_dbs_16_numa",
             "intmq_c_4_k_4_ibs_16_dbs_16_numa",
             "intmq_c_8_k_8_ibs_16_dbs_16_numa", "wrapper_linden",
             "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024",
             "wrapper_capq")
  labels <- c("MultiQueue (2, 1)", "MultiQueue (4, 1)",   "MultiQueue (4, 4)", "MultiQueue (8, 8)", "linden", "spraylist", "klsm256", "klsm1024", "capq")
  # names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_16_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_8_ns_128_numa", "mergingmq_c_8_k_8_ns_128_numa", "mergingmq_c_16_k_16_ns_128_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Delay", y = "Cumulative Frequency") +
        scale_color_manual(breaks = names, values = c(3, 3, 3, 3, 2, 4, 6, 6, 7), labels = labels) +
        scale_linetype_manual(breaks = names, values = c(1, 2, 3, 4, 1, 1, 1, 2, 1), labels = labels) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), axis.title.y = element_blank(), axis.text.y=element_blank(), axis.ticks.y = element_blank())
      ggsave(plot, file = paste(outdir, "/", "delay_compare_", .$threads[1], ".pdf", sep = ""), width = 5, height = 2.5)
      .
    })
}

plot_throughput_comparison <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa", "intmq_c_8_k_8_ibs_16_dbs_16_numa", "wrapper_linden", "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq")
  labels <- c("MultiQueue (2, 1)", "MultiQueue (4, 1)",   "MultiQueue (4, 4)", "MultiQueue (8, 8)", "linden", "spraylist", "klsm256", "klsm1024", "capq")
  # names <- c("fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa", "fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa", "fullbufferingmq_c_16_k_16_ibs_16_dbs_16_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_8_ns_128_numa", "mergingmq_c_8_k_8_ns_128_numa", "mergingmq_c_16_k_16_ns_128_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data$name <- factor(plot_data$name, levels = names)
  plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name, linetype = name, shape = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = bquote(10^6 ~ "Ops/s")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
    scale_color_manual(breaks = names, values = c(3, 3, 3,  3, 2, 4, 6, 6, 7), labels = labels) +
    scale_linetype_manual(breaks = names, values = c(1, 2, 3, 4, 1, 1, 1, 2, 1), labels = labels) +
    scale_shape_manual(breaks = names, values = 1:9, labels = labels) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(legend.position = "none", panel.grid.major.x = element_blank())
    # theme(panel.grid.major.x = element_blank(), legend.position = "none")
    ggsave(plot, file = paste(outdir, "/throughput_compare.pdf", sep = ""), width = 5, height = 4)
}

prefill_labeller <- function(variable, value) {
  paste("n =", value)
}

plot_rank_comparison_by_prefill <- function(data, outdir) {
  names <- c("wrapper_linden", "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa")
  labels <- c("linden", "spraylist", "klsm256", "klsm1024", "capq", "MultiQueue c=4, s=1",  "MultiQueue c=4, s=4")
  plot_data <- data %>% filter(name %in% names & dist == "uniform" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, group = interaction(name, prefill), color = name)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        geom_line() +
        coord_cartesian(xlim = c(1, 20000)) +
        scale_color_manual(breaks = names, values = c(2:8), labels = labels) +
        guides(color = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), strip.background = element_blank()) +
        facet_wrap(~prefill, ncol = 1, labeller=prefill_labeller) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/rank_compare_prefill_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

plot_rank_comparison_by_dist <- function(data, outdir) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq")
  labels <- c("MultiQueue (2, 1)", "MultiQueue (4, 1)",   "MultiQueue (4, 4)", "klsm256", "klsm1024", "capq")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      print(.$name)
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, group = interaction(name, dist), color = name)) +
        geom_line() +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 20000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(breaks = names, values = c(3, 3, 3, 6, 6, 7), labels = labels) +
        scale_linetype_manual(breaks = names, values = c(1, 2, 3, 1, 2, 1), labels = labels) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = "right") +
        facet_wrap(~dist, ncol = 1)
      ggsave(plot, file = paste(outdir, "/rank_compare_dist_", .$threads[1], ".pdf", sep = ""), width = 5, height = 5)
      .
    })
}

plot_delay_comparison_by_prefill <- function(data, outdir) {
  names <- c("wrapper_linden", "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa")
  labels <- c("linden", "spraylist", "klsm256", "klsm1024", "capq", "MultiQueue c=4, s=1",  "MultiQueue c=4, s=4")
  plot_data <- data %>% filter(name %in% names & dist == "uniform" & sleep == "0")

  plot_data %>%
    group_by(threads) %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, group = interaction(name, prefill), color = name)) +
        labs(x = "Delay", y = "Cumulative Frequency") +
        geom_line() +
        coord_cartesian(xlim = c(1, 20000)) +
        scale_color_manual(breaks = names, values = c(2:8), labels = labels) +
        guides(color = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), strip.background = element_blank()) +
        facet_wrap(~prefill, ncol = 1, labeller=prefill_labeller) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/delay_compare_prefill_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

plot_delay_comparison_by_dist <- function(data, outdir) {
  names <- c("wrapper_linden", "wrapper_spraylist", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa")
  labels <- c("linden", "spraylist", "klsm256", "klsm1024", "capq", "MultiQueue c=4, s=1",  "MultiQueue c=4, s=4")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & sleep == "0")
  plot_data %>%
    group_by(threads) %>%
    do({
      print(plot_data)
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, group = interaction(name, dist), color = name)) +
        labs(x = "Delay", y = "Cumulative Frequency") +
        geom_line() +
        coord_cartesian(xlim = c(1, 20000)) +
        scale_color_manual(breaks = names, values = c(2:8), labels = labels) +
        guides(color = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), strip.background = element_blank()) +
        facet_wrap(~dist, ncol = 1) +
        scale_x_log10()
      ggsave(plot, file = paste(outdir, "/delay_compare_dist_", .$threads[1], ".pdf", sep = ""), width = 6, height = 5)
      .
    })
}

plot_sssp <- function(data, outdir, graph_name) {
  names <- c("intmq_c_2_k_1_ibs_16_dbs_16_numa", "intmq_c_4_k_1_ibs_16_dbs_16_numa",  "intmq_c_4_k_4_ibs_16_dbs_16_numa",   "intmq_c_8_k_8_ibs_16_dbs_16_numa", "wrapper_linden", "wrapper_klsm256", "wrapper_klsm1024", "wrapper_capq")
  labels <- c("MultiQueue (2, 1)", "MultiQueue (4, 1)",   "MultiQueue (4, 4)",    "MultiQueue (8, 8)", "linden", "klsm256", "klsm1024", "capq")
  plot_data <- data %>% filter(name %in% names & graph == graph_name)
  plot <- ggplot(plot_data, aes(x = threads, y = time, color = name, linetype = name, shape = name)) +
    geom_line() +
    geom_point() +
    labs(x = "p", y = bquote("Time [ms]")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
    scale_y_continuous(limits=c(0, NA)) +
    scale_color_manual(breaks = names, values = c(3, 3, 3, 3, 2, 6, 6, 7), labels = labels) +
    scale_linetype_manual(breaks = names, values = c(1, 1, 1, 1, 1, 1, 1, 1), labels = labels) +
    scale_shape_manual(breaks = names, values = c(1, 2, 3, 4, 8, 7, 6, 5), labels = labels) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(panel.grid.major.x = element_blank(), legend.position = "none")
  ggsave(plot, file = paste(outdir, "/sssp_time_", graph_name, ".pdf", sep = ""), width = 5, height = 3)
  plot <- ggplot(plot_data, aes(x = threads, y = relaxed / 1000000, color = name, linetype = name, shape = name)) +
    geom_line() +
    geom_point() +
    labs(x = "p", y = bquote("Relaxed nodes")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 100) , minor_breaks = NULL, limits=c(1, 100), oob = squish, trans="log2") +
    # scale_y_continuous(labels = c("3.8m", "4.0m", "4.2m", "4.4m", "4.6m"), breaks = c(3.8, 4, 4.2, 4.4, 4.6)) +
    scale_color_manual(breaks = names, values = c(3, 3, 3, 3, 2, 6, 6, 7), labels = labels) +
    scale_linetype_manual(breaks = names, values = c(1, 1, 1, 1, 1, 1, 1, 1), labels = labels) +
    scale_shape_manual(breaks = names, values = 1:8, labels = labels) +
    guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(legend.position = "right", panel.grid.major.x = element_blank())
  ggsave(plot, file =paste(outdir, "/sssp_nodes_", graph_name, ".pdf", sep = ""), width = 6, height = 3)
}

plot_throughput_icx <- function(data, outdir) {
  # names <- c("mergingmq_c_4_k_1_ns_8_numa", "mergingmq_c_4_k_1_ns_64_numa", "mergingmq_c_4_k_1_ns_128_numa", "mergingmq_c_4_k_1_ns_512", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa")
  names <- c("mergingmq_c_8_k_8_ns_128_numa", "fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform")
  plot_data$name <- factor(plot_data$name, levels = names)
  plot <- ggplot(plot_data, aes(x = threads, y = mean, color = name, shape = name)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = mean - sd, ymax = mean + sd),
      width = .2,
      position = position_dodge(0.05)
    ) +
    labs(x = "p", y = bquote(10^6 ~ "Ops/s")) +
    scale_x_continuous("p", labels = c("1", "2", "4", "8", "16", "32", "64", "64 ht"), breaks = c(1, 2, 4, 8, 16, 32, 64, 80) , minor_breaks = NULL, limits=c(1, 80), oob = squish) +
    # scale_color_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
    # scale_color_manual(values = c(2, 3, 4, 6, 7), labels = c("no buffering", "b = 4", "b = 8", "b = 16", "b = 64")) +
    guides(color = guide_legend(title = NULL), shape = guide_legend(title = NULL)) +
    theme_bw() +
    theme(legend.position = c(0.15, 0.82))
    ggsave(plot, file = paste(outdir, "/throughput_buffer_size.pdf", sep = ""), width = 6, height = 5)
}

plot_rank_with_old_theory <- function(data, outdir) {
  names <- c("old_c_4", "orig_c_4_bb", "orig_c_4_ba", "orig_c_4_ab", "orig_c_4_aa", "theo 4")
  labels <- c("original", "before insert, before delete", "before insert, after delete", "after insert, before delete", "after insert, after delete", "theory")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        # stat_function(fun = function(x) (1 - 2 / (2 * .$threads[1]))^(x - 1), aes(color = "theo 2", linetype = "theo 2")) +
        stat_function(fun = function(x) (1 - 2 / (4 * 56))^(x - 1), aes(color = "theo 4", linetype = "theo 4")) +
        # stat_function(fun = function(x) (1 - 2 / (8 * .$threads[1]))^(x - 1), aes(color = "theo 8", linetype = "theo 8")) +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 5000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(breaks = names, values = c(1, 2, 3, 4, 5, 6), labels = labels) +
        scale_linetype_manual(breaks = names, values = c(1, 2, 3, 4, 5, 6), labels = labels) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.80, 0.80))
      ggsave(plot, file = paste(outdir, "/", "rank_with_old_theory_c_4.pdf", sep = ""), width = 6, height = 5)
      .
    })
  names <- c("old_c_2", "orig_c_2_bb", "orig_c_2_ba", "orig_c_2_ab", "orig_c_2_aa", "theo 2")
  labels <- c("original", "before insert, before delete", "before insert, after delete", "after insert, before delete", "after insert, after delete", "theory")
  plot_data <- data %>% filter(name %in% names & prefill == "1000000" & dist == "uniform" & sleep == "0")
  plot_data %>%
    do({
      plot <- ggplot(., aes(x = rank + 1, y = cumulated, color = name, linetype = name)) +
        geom_line() +
        # stat_function(fun = function(x) (1 - 2 / (2 * .$threads[1]))^(x - 1), aes(color = "theo 2", linetype = "theo 2")) +
        stat_function(fun = function(x) (1 - 2 / (2 * 56))^(x - 1), aes(color = "theo 2", linetype = "theo 2")) +
        # stat_function(fun = function(x) (1 - 2 / (8 * .$threads[1]))^(x - 1), aes(color = "theo 8", linetype = "theo 8")) +
        scale_x_log10() +
        coord_cartesian(xlim = c(1, 5000)) +
        labs(x = "Rank", y = "Cumulative Frequency") +
        scale_color_manual(breaks = names, values = c(1, 2, 3, 4, 5, 6), labels = labels) +
        scale_linetype_manual(breaks = names, values = c(1, 2, 3, 4, 5, 6), labels = labels) +
        guides(color = guide_legend(title = NULL), linetype = guide_legend(title = NULL)) +
        theme_bw() +
        theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), legend.position = c(0.80, 0.80))
      ggsave(plot, file = paste(outdir, "/", "rank_with_old_theory_c_2.pdf", sep = ""), width = 6, height = 5)
      .
    })
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

  # plot_rank_histogram_int(data$rank, dir)
  # plot_rank_histogram_theory_wrapped(data$rank, dir)
  # plot_throughput_buffer_size(data$throughput, dir)
  plot_rank_with_old_theory(data$rank, dir)
  # plot_delay(data$delay, c("nobufferingmq_numa", "fullbufferingmq_c_4_k_1_ibs_4_dbs_4_numa", "fullbufferingmq_c_4_k_1_ibs_8_dbs_8_numa", "fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa",  "fullbufferingmq_c_4_k_1_ibs_64_dbs_64_numa"), dir)
  # plot_rank(data$rank, c("steady", "realtime", "rdtsc", "long_sleep"), dir)
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


# experiment_dir <- "./experiments"
# read_histogram("rank_histogram56.txt", "", "", "", "", "")
# args <- commandArgs(trailingOnly = T)
# path <- paste(experiment_dir, args[1], sep = "/")
# print(paste("Reading results for", path))
# if (dir.exists(path)) {
#   data <- read_scenario(paste(path, "results", sep = "/"))
#   print("done")
#   plot_scenario(data, path)
# } else {
#   print("Result directory not found")
# }
