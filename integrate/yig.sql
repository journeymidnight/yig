-- MySQL dump 10.13  Distrib 5.7.21, for Linux (x86_64)
--
-- Host: localhost    Database: yig
-- ------------------------------------------------------
-- Server version	5.7.21

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
  `bucketname` varchar(100) CHARACTER SET latin1 NOT NULL DEFAULT '',
  `acl` varchar(30) CHARACTER SET latin1 DEFAULT NULL,
  `cors` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `lc` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `uid` varchar(16) CHARACTER SET latin1 DEFAULT NULL,
  `createtime` datetime DEFAULT NULL,
  `usages` bigint(20) DEFAULT NULL,
  `versioning` varchar(20) CHARACTER SET latin1 DEFAULT NULL,
  PRIMARY KEY (`bucketname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
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
  `objectid` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `lastmodified` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `initializationvector` blob
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objects`
--

DROP TABLE IF EXISTS `objects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `objects` (
  `bucketname` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `name` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `version` bigint(20) unsigned DEFAULT NULL,
  `location` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `pool` varchar(30) CHARACTER SET latin1 DEFAULT NULL,
  `ownerId` varchar(20) CHARACTER SET latin1 DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(30) CHARACTER SET latin1 DEFAULT NULL,
  `lastmodifiedtime` datetime DEFAULT NULL,
  `etag` varchar(50) CHARACTER SET latin1 DEFAULT NULL,
  `contenttype` varchar(50) CHARACTER SET latin1 DEFAULT NULL,
  `customattributes` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `acl` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `nullversion` tinyint(1) DEFAULT NULL,
  `deletemarker` tinyint(1) DEFAULT NULL,
  `ssetype` varchar(20) CHARACTER SET latin1 DEFAULT NULL,
  `encryptionkey` blob,
  `initializationvector` blob,
  KEY `search` (`bucketname`,`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objmap`
--

DROP TABLE IF EXISTS `objmap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `objmap` (
  `bucketname` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `objectname` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `nullvernum` bigint(20) DEFAULT NULL,
  KEY `objmap` (`bucketname`,`objectname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `userid` varchar(50) CHARACTER SET latin1 DEFAULT NULL,
  `bucketname` varchar(50) CHARACTER SET latin1 DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-02-27  7:25:07
