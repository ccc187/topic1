CREATE TABLE `topic_details` (
  `id` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `title` varchar(255) NOT NULL,
  `bg_pic` longtext NOT NULL,
  `avatar` longtext NOT NULL,
  `sort` int(11) NOT NULL,
  `catalogue` longtext NOT NULL,
  `start_at` datetime(3) NOT NULL,
  `end_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_topic_details_created_at` (`created_at`),
  KEY `idx_topic_details_deleted_at` (`deleted_at`),
  UNIQUE KEY `idx_topic_details_title` (`title`),
  KEY `idx_topic_details_sort` (`sort`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;