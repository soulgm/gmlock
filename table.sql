CREATE TABLE `shedlock`
(
    `name`       varchar(64) NOT NULL,
    `lock_until` timestamp(3) NULL DEFAULT NULL,
    `locked_at`  timestamp(3) NULL DEFAULT NULL,
    `locked_by`  varchar(255)         DEFAULT NULL,
    `op_id`      bigint      NOT NULL DEFAULT '0' COMMENT '附身id',
    PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='分布锁定时使用'
;
