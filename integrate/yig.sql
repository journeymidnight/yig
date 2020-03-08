-- MySQL dump 10.14  Distrib 5.5.56-MariaDB, for Linux (x86_64)
--
-- Host: 10.5.0.17    Database: yig
-- ------------------------------------------------------
-- Server version	5.7.10-TiDB-v2.0.0-rc.1-71-g7f958c5

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `buckets`
--

DROP TABLE IF EXISTS `buckets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `buckets` (
  `bucketname` varchar(255) NOT NULL DEFAULT '',
  `acl` JSON DEFAULT NULL,
  `cors` JSON DEFAULT NULL,
  `logging` JSON NOT NULL DEFAULT '',
  `lc` JSON DEFAULT NULL,
  `uid` varchar(255) DEFAULT NULL,
  `policy` JSON DEFAULT NULL,
  `website` JSON DEFAULT NULL,
  `encryption` JSON DEFAULT NULL,
  `createtime` datetime DEFAULT NULL,
  `usages` bigint(20) DEFAULT NULL,
  `versioning` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`bucketname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster`
--

DROP TABLE IF EXISTS `cluster`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster` (
  `fsid` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`fsid`,`pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `gc`
--

DROP TABLE IF EXISTS `gc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gc` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `part` tinyint(1) DEFAULT NULL,
  `triedtimes` int(11) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `gcpart`
--

DROP TABLE IF EXISTS `gcpart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gcpart` (
  `partnumber` int(11) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `lastmodified` datetime DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
   KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `multipartpart`
--

DROP TABLE IF EXISTS `multipartpart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `multipartpart` (
  `partnumber` int(11) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `lastmodified` datetime DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `uploadtime` bigint(20) UNSIGNED DEFAULT NULL,
   KEY `rowkey` (`bucketname`,`objectname`,`uploadtime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `multiparts`
--

DROP TABLE IF EXISTS `multiparts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  `cipher` blob DEFAULT NULL,
  `attrs` JSON DEFAULT NULL,
  `storageclass` tinyint(1) DEFAULT 0,
  UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`uploadtime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objectpart`
--

DROP TABLE IF EXISTS `objectpart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `objectpart` (
  `partnumber` int(11) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `lastmodified` datetime DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
   KEY `rowkey` (`bucketname`,`objectname`,`version`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objects`
--

DROP TABLE IF EXISTS `objects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  `type` tinyint(1) DEFAULT 0,
  `storageclass` tinyint(1) DEFAULT 0,
   UNIQUE KEY `rowkey` (`bucketname`,`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `restoreobjectpart`
--

DROP TABLE IF EXISTS `restoreobjectpart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `restoreobjectpart` (
  `partnumber` int(11) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `lastmodified` datetime DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) unsigned DEFAULT NULL,
  KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `restoreobjects`
--

DROP TABLE IF EXISTS `restoreobjects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `restoreobjects` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) unsigned DEFAULT NULL,
  `status` tinyint(1) DEFAULT '0',
  `lifetime` tinyint(2) DEFAULT '1',
  `lastmodifiedtime` datetime DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `ownerid` varchar(255) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objmap`
--

DROP TABLE IF EXISTS `objmap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `objmap` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `nullvernum` bigint(20) DEFAULT NULL,
  UNIQUE KEY `objmap` (`bucketname`,`objectname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `userid` varchar(255) DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-03-20 18:26:36


DROP TABLE IF EXISTS `lifecycle`;
CREATE TABLE `lifecycle` (
                       `bucketname` varchar(255) DEFAULT NULL,
                       `status` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;