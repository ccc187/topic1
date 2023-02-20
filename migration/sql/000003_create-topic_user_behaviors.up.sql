CREATE TABLE `topic_user_behaviors` (
  `id` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  `deleted_at` datetime(3) DEFAULT NULL,
  `topic_id` bigint(20) DEFAULT NULL,
  `user_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_topic_user_behaviors_created_at` (`created_at`),
  KEY `idx_topic_user_behaviors_deleted_at` (`deleted_at`),
  UNIQUE KEY `topicIdUserID` (`topic_id`,`user_id`),
  KEY `idx_topic_user_behaviors_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;