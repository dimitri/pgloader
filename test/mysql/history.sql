CREATE TABLE `history` (
  `hotel_id` varchar(16) NOT NULL,
  `update_type` varchar(255) NOT NULL,
  `code` varchar(255) DEFAULT NULL,
  `affected_from` date DEFAULT NULL,
  `affected_to` date DEFAULT NULL,
  `submit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `submit_ip` varchar(15) DEFAULT NULL,
  `submit_user` varchar(255) DEFAULT NULL,
  `id` bigint(20) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


ALTER TABLE `history`
  ADD PRIMARY KEY (`id`) USING BTREE,
  ADD KEY `update_type` (`update_type`),
  ADD KEY `hotel_id` (`hotel_id`);

ALTER TABLE `history`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

