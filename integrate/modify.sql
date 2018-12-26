-- rename table

ALTER TABLE `multiparts`
	RENAME TO `multiparts_bak`;

-- create new table

CREATE TABLE `multiparts` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `uploadtime` bigint(20) UNSIGNED DEFAULT NULL,
  `initiatorid` varchar(255) DEFAULT NULL,
  `ownerid` varchar(255) DEFAULT NULL,
  `contenttype` varchar(255) DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `acl` JSON DEFAULT NULL,
  `sserequest` JSON DEFAULT NULL,
  `encryption` blob DEFAULT NULL,
  `attrs` JSON DEFAULT NULL,
  UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`uploadtime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- copy data

INSERT INTO `multiparts` SELECT * FROM `multiparts_bak`;

ALTER TABLE `objects`
	RENAME TO `objects_bak`;

CREATE TABLE `objects` (
  `bucketname` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `ownerid` varchar(255) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `lastmodifiedtime` datetime DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `contenttype` varchar(255) DEFAULT NULL,
  `customattributes` JSON DEFAULT NULL,
  `acl` JSON DEFAULT NULL,
  `nullversion` tinyint(1) DEFAULT NULL,
  `deletemarker` tinyint(1) DEFAULT NULL,
  `ssetype` varchar(255) DEFAULT NULL,
  `encryptionkey` blob DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
   UNIQUE KEY `rowkey` (`bucketname`,`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

INSERT INTO `objects` SELECT * FROM `objects_bak`;