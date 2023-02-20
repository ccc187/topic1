CREATE TABLE `topic_statistics` (
  `id` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `topic_id` bigint(20) NOT NULL,
  `content_num` bigint(20) NOT NULL,
  `mp_num` bigint(20) NOT NULL,
  `content_exposure_num` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_topic_statistics_created_at` (`created_at`),
  KEY `idx_topic_statistics_deleted_at` (`deleted_at`),
  UNIQUE KEY `idx_topic_statistics_topic_id` (`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;