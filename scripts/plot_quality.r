library(ggplot2)
library(dplyr)
library(reshape2)
library(stringr)

experiment_dir <- "./experiments"

read_histogram <- function(file, name) {
  if (file.exists(file)) {
  data <- read.csv(file = file, header = F, col.names = c("rank", "frequency"), sep = " ")
  data$name <- name
  data["cummulated"] <- rev(cumsum(rev(data$frequency)))
  data$cummulated <- data$cummulated / data$cummulated[1]
  data
  # } else {
  #   setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("rank", "frequency", "name", "cummulated"))
  }
}

read_histogramlist <- function(files, histogram) {
  if (length(files) == 0) {
    data <- data.frame(matrix(ncol = 6, nrow = 0))
    colnames(data) <- c("rank", "frequency", "name", "cummulated")
    return(data)
  }
  hist_list <- list()
  i <- 1
  for (f in files) {
    data <- read_histogram(paste(f, "/", histogram, ".txt", sep = ""), f)
    hist_list[[i]] <- data
    i <- i + 1
  }
  data <- do.call(rbind, hist_list)
  return(data)
}

read_throughput <- function(file, name) {
  if (file.exists(file)) {
  data <- read.csv(file = file, header = T, sep = " ")
  data <- data %>%
    group_by(threads) %>%
    summarize(mean = mean(throughput / 1000000), sd = sd(throughput / 1000000))
  data$name <- name
  return(data)
  }
  data <- data.frame(matrix(ncol = 4, nrow = 0))
  colnames(data) <- c("threads", "mean", "sd", "name")
  return(data)
}


plot_histogram <- function(data, outdir, plotname) {
  # print(head(data[data$name == "fullbufferingmq", ]))
  plot_data <- data[data$name %in% c("wrapper_capq", "mergingmq", "nummq", "fullbufferingmq_c_4_k_4", "fullbufferingmq_c_4_k_64"), ]
  plot_data %>% group_by(threads) %>% do({
  plot <- ggplot(., aes(x = rank, y = cummulated, group = name, color = name)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Rank") +
    geom_line() +
    scale_color_brewer(palette = "Set1") +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/", plotname, "_", .$threads[1],".pdf", sep = ""))
  .
  })
}

plot_histogram_by_stickiness <- function(data, outdir, plotname) {
  ss <- data[startsWith(data$name, "fullbufferingmq_c"), ]
  ss$c <- as.factor(str_extract(ss$name, "(?<=c_)\\d+(?=_k)"))
  ss$c <- ordered(ss$c, levels = c("4", "8", "16"))
  ss$k <- as.factor(str_extract(ss$name, "(?<=k_)\\d+$"))
  ss$k <- ordered(ss$k, levels = c("2", "4", "8", "16", "64"))
  ss %>% group_by(threads) %>% do({
  plot <- ggplot(., aes(x = rank, y = cummulated, group = interaction(c, k), color = k)) +
    labs(x = "Rank", y = "Cummul. Frequency", title = "Rank") +
    geom_line() +
    scale_color_brewer(palette = "Set1") +
    facet_wrap(~c, ncol = 1) +
    scale_x_log10()
  ggsave(plot, file = paste(outdir, "/", plotname, "_", .$threads[1], ".pdf", sep = ""))
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

plot_throughput_by_thread <- function(data, outdir) {
  # plot <- ggplot(data[data$name %in% c("nobufferingmq", "fullbufferingmq", "insertionbuffermq", "mergingmq", "numamq", "numamergingmq", "wrapper_capq", "wrapper_dlsm", "wrapper_klsm"), ], aes(x = threads, y = mean, group = name, color = name)) +
  plot <- ggplot(data[data$name %in% c("wrapper_capq", "mergingmq", "nummq", "fullbufferingmq_c_4_k_4", "fullbufferingmq_c_4_k_64"), ], aes(x = threads, y = mean, group = name, color = name)) +
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
  ss <- data[startsWith(data$name, "fullbufferingmq_ib"), ]
  ss$ibs <- as.factor(str_extract(ss$name, "(?<=ib_)\\d+(?=_db)"))
  ss$ibs <- ordered(ss$ibs, levels = c("4", "8", "16", "64"))
  ss$dbs <- as.factor(str_extract(ss$name, "(?<=db_)\\d+$"))
  ss$dbs <- ordered(ss$dbs, levels = c("4", "8", "16", "64"))
  plot <- ggplot(ss, aes(x = threads, y = mean, group = interaction(ibs, dbs), color = dbs)) +
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
  ss <- data[startsWith(data$name, "fullbufferingmq_c"), ]
  ss$c <- as.factor(str_extract(ss$name, "(?<=c_)\\d+(?=_k)"))
  ss$c <- ordered(ss$c, levels = c("4", "8", "16"))
  ss$k <- as.factor(str_extract(ss$name, "(?<=k_)\\d+$"))
  ss$k <- ordered(ss$k, levels = c("2", "4", "8", "16", "64"))
  plot <- ggplot(ss, aes(x = threads, y = mean, group = interaction(c, k), color = k)) +
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
  ss <- data[startsWith(data$name, "mergingmq_ns"), ]
  ss$ns <- as.factor(str_extract(ss$name, "(?<=ns_)\\d+$"))
  ss$ns <- ordered(ss$ns, levels = c("2", "4", "8", "16", "64", "128", "512"))
  plot <- ggplot(ss, aes(x = threads, y = mean, group = ns, color = ns)) +
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

plot_scenario <- function(data, dir) {
  plot_histogram(data$rank, dir, "rank")
  plot_histogram_by_stickiness(data$rank, dir, "rank_by_stickiness")
  plot_histogram(data$rank, dir, "delay")
  plot_histogram_by_stickiness(data$rank, dir, "delay_by_stickiness")
  # plot_histogram_bar(data$top_delay, dir, "top_delay")
  plot_throughput_by_thread(data$throughput, dir)
  plot_throughput_by_buffer_size(data$throughput, dir)
  plot_throughput_by_stickiness(data$throughput, dir)
  plot_throughput_by_ns(data$throughput, dir)
}

read_scenario <- function(scenario) {
  scenario_data <- list()
  rank_list <- list()
  delay_list <- list()
  top_delay_list <- list()
  throughput_list <- list()
  i <- 1
  for (pq in list.dirs(path = paste(experiment_dir, scenario, sep = "/"), full.names = T, recursive = F)) {
    name <- gsub(".*/", "", pq)
    # if (name %in% c("nobufferingmq", "fullbufferingmq", "insertionbuffermq", "deletionbuffermq", "mergingmq", "numamq", "numamergingmq", "wrapper_linden", "wrapper_capq", "wrapper_dlsm", "wrapper_spraylist")) {
    for (j in c(1, 2, 4, 8, 16)) {
      rank_data <- read_histogram(paste(pq, "/rank_", j, ".txt", sep = ""), name)
      rank_data$threads <- j
      rank_list[[i]] <- rank_data
      delay_data <- read_histogram(paste(pq, "/delay_", j, ".txt", sep = ""), name)
      delay_data$threads <- j
      delay_list[[i]] <- delay_data
      top_delay_data <- read_histogram(paste(pq, "/top_delay_", j, "16.txt", sep = ""), name)
      top_delay_data$threads <- j
      top_delay_list[[i]] <- top_delay_data
      i <- i + 1
    }
    throughput_data <- read_throughput(paste(pq, "/throughput.txt", sep = ""), name)
    throughput_list[[i]] <- throughput_data
    i <- i + 1
  }
  rank_data <- do.call(rbind, rank_list)
  delay_data <- do.call(rbind, delay_list)
  top_delay_data <- do.call(rbind, top_delay_list)
  throughput_data <- do.call(rbind, throughput_list)
  list("rank" = rank_data, "delay" = delay_data, "top_delay" = top_delay_data, "throughput" = throughput_data)
}

# data <- read_scenario("prefill")
# plot_scenario(data, paste(experiment_dir, "prefill", sep = "/"))
# plot_throughput_by_prefill(data$throughput, paste(experiment_dir, "prefill", sep = "/"))

# data <- read_scenario("distribution")
# plot_scenario(data, paste(experiment_dir, "distribution", sep = "/"))
# plot_throughput_by_dist(data$throughput, paste(experiment_dir, "distribution", sep = "/"))

data <- read_scenario("i10pc137/results")
plot_scenario(data, paste(experiment_dir, "i10pc137", sep = "/"))
# plot_throughput_by_dist(data$throughput, paste(experiment_dir, "distribution", sep = "/"))

# data <- read_scenario("buf_size")
# plot_throughput_by_buffer_size(data$throughput, paste(experiment_dir, "buf_size", sep = "/"))

# data <- read_scenario("c")
# plot_throughput_by_c(data$throughput, paste(experiment_dir, "c", sep = "/"))

# data <- read_scenario("node_size")
# plot_throughput_by_ns(data$throughput, paste(experiment_dir, "node_size", sep = "/"))
