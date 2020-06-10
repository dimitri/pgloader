create table `empty`(id integer auto_increment primary key);

CREATE TABLE `races` (
  `raceId` int(11) NOT NULL AUTO_INCREMENT,
  `year` int(11) NOT NULL DEFAULT 0,
  `round` int(11) NOT NULL DEFAULT 0,
  `circuitId` int(11) NOT NULL DEFAULT 0,
  `name` varchar(255) NOT NULL DEFAULT '',
  `date` date NOT NULL DEFAULT '0000-00-00',
  `time` time DEFAULT NULL,
  `url` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`raceId`),
  UNIQUE KEY `url` (`url`)
) ENGINE=MyISAM AUTO_INCREMENT=989 DEFAULT CHARSET=utf8;

CREATE TABLE `utilisateurs__Yvelines2013-06-28` (
  `statut` enum('administrateur','odis','pilote','bureau','relais','stagiaire','membre','participant','contact') COLLATE utf8_unicode_ci NOT NULL,
  `anciennete` year(4) DEFAULT NULL,
  `sexe` enum('H','F') COLLATE utf8_unicode_ci DEFAULT NULL,
  `categorie` enum('Exploitant agricole','Artisan, commerçant, chef d''entreprise','Cadre, profession intellectuelle supérieure','Profession intermédiaire','Employé','Ouvrier','Retraité','Étudiant','Sans activité professionnelle') COLLATE utf8_unicode_ci DEFAULT NULL,
  `secteur` enum('Secteur privé - Industrie','Secteur privé - Services','Secteur privé - Agriculture','Fonction publique d''Etat','Fonction publique hospitalière','Fonction publique territoriale','Secteur associatif et social') COLLATE utf8_unicode_ci DEFAULT NULL,
  `nombre` enum('0','1','2','3 ?? 5','6 ?? 9','10 ?? 19','20 ?? 49','50 ?? 99','100 ?? 199','200 ?? 499','500 ?? 999','1 000 ?? 4 999','5 000 ?? 9 999','10 000 et plus') COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

create table `geom`
 (
   id integer auto_increment primary key,
   p  point,
   l  linestring
 );

insert into `geom`(`p`, `l`)
     values (ST_GeomFromText('POINT(1 1)'),
             ST_GeomFromText('LINESTRING(-87.87342467651445 45.79684462673078,-87.87170806274479 45.802110434248966,-87.84492888794016 45.79732335706963,-87.84046569213925 45.79349339919981,-87.83703246459994 45.795887153715135,-87.82261290893646 45.7954084110377,-87.82261290893646 45.781523084289475,-87.82879271850615 45.7796075953549,-87.84115233764729 45.779128712838755,-87.85110869751074 45.78008647375808,-87.83737578735439 45.782001946240946,-87.84527221069374 45.78343850741725)'));

/*
 * https://github.com/dimitri/pgloader/issues/650
 */
CREATE TABLE propertydecimal (
  ID decimal(18,0) NOT NULL,
  propertyvalue decimal(18,6) DEFAULT NULL,
  PRIMARY KEY (ID)
);

create table `base64`
 (
   id   varchar(255),
   data longtext
 )
 comment "Test decoding base64 documents";

insert into `base64`(id, data)
     values('65de699d-b5cc-4e13-b507-c71adea31e53',
            'eyJrZXkiOiAidmFsdWUifQ==');

CREATE TABLE `onupdate` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `patient_id` varchar(50) NOT NULL,
  `calc_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `patient_id` (`patient_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

/* https://github.com/dimitri/pgloader/issues/661 */
CREATE TABLE funny_string AS select char(41856 using 'gbk') AS s;

/* https://github.com/dimitri/pgloader/issues/664 */

/*
CREATE TABLE `table_name` (
  `field1` double NOT NULL AUTO_INCREMENT,
  `field2` varchar(50) DEFAULT NULL,
  `field3` int(11) DEFAULT NULL,
  `field4` tinyint(3) DEFAULT NULL,
  PRIMARY KEY (`field1`),
  KEY `idx_field3` (`field3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPACT;
*/

/*
 * https://github.com/dimitri/pgloader/issues/678
 */
CREATE TABLE pgloader_test_unsigned
(
  id SMALLINT UNSIGNED,
  sm smallint,
  tu TINYINT UNSIGNED
);
INSERT INTO pgloader_test_unsigned(id) VALUES (65535);

/*
 * https://github.com/dimitri/pgloader/issues/684
 */
create table bits
 (
  id   integer not null AUTO_INCREMENT primary key,
  bool bit(1)
 );

insert into bits(bool) values(0b00), (0b01);

/*
 * https://github.com/dimitri/pgloader/issues/811
 */
CREATE TABLE `domain_filter` (
  `id` binary(16) NOT NULL ,
  `type` varchar(50) NOT NULL ,
  `value` json DEFAULT NULL ,
  `negated` tinyint(1) NOT NULL DEFAULT '0' ,
  `report_id` varbinary(255) NOT NULL ,
  `query_id` varchar(255) NOT NULL ,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  `updated_at` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP ,
  `updated_by` varbinary(255) DEFAULT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `domain_filter_unq` (`report_id`,`query_id`,`type`),
  KEY `domain_filter` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii;

/*
 * https://github.com/dimitri/pgloader/issues/904
 */
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `encryption_key_canary` (
  `encrypted_value` blob,
  `nonce` tinyblob,
  `uuid` binary(16) NOT NULL,
  `salt` tinyblob,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `encryption_key_canary`
--

LOCK TABLES `encryption_key_canary` WRITE;
/*!40000 ALTER TABLE `encryption_key_canary` DISABLE KEYS */;
INSERT INTO `encryption_key_canary` VALUES (
  0x1F36F183D7EE47C71453850B756945C16D9D711B2F0594E5D5E54D1EC94E081716AB8642AA60F84B50F69454D098122B7136A0DEB3AF200C2C5C7500BDFA0BD9689CCBF10A76972374882B304F7F15A227E815989FC87EEB72612396F569C662E72A2A7555E654605A3B83C1C753297832E52C5961E81EBC60DC43D929ABAB8CB14601DEFED121604CEB26210AB6D724,
  0x044AA707DF17021E55E9A1E4,
  0x88C2982F428A46B7B71B210618AE1658,
  0xAE7F18028E7984FB5630F7D23FB77999C6CA7CF5355EF0194F3F16521EA7EC503F566229ED8DC5EFBBE9C12BA491BDDC939FE60FA31FB9AF123B2B4D5B7A61FE
);
/*!40000 ALTER TABLE `encryption_key_canary` ENABLE KEYS */;
UNLOCK TABLES;


/*
 * https://github.com/dimitri/pgloader/issues/703
 */
create table `CamelCase` (
 `validSizes` varchar(12)
);

/*
 * https://github.com/dimitri/pgloader/issues/943
 */
CREATE TABLE `countdata_template`
(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data` int(11) DEFAULT NULL,
  `date_time` datetime DEFAULT NULL,
  `gmt_offset` smallint(6) NOT NULL DEFAULT '0' COMMENT 'Offset GMT en minute',
  `measurement_id` int(11) NOT NULL,
  `flags` bit(16) NOT NULL DEFAULT b'0' COMMENT 'mot binaire : b1000=validé, b10000000=supprimé',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ak_countdata_idx` (`measurement_id`,`date_time`,`gmt_offset`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='données de comptage';

INSERT INTO `countdata_template`(`date_time`, `measurement_id`, `flags`)
     VALUES (now(), 1, b'1000'),
            (now(), 2, b'10000000');


/*
 * https://github.com/dimitri/pgloader/issues/1102
 */
CREATE TABLE `uw_defined_meaning` (
  `defined_meaning_id` int(8) unsigned NOT NULL,
  `expression_id` int(10) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `fcm_batches` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `raw_payload` mediumtext COLLATE utf8_unicode_ci,
  `success` int(10) unsigned NOT NULL DEFAULT '0',
  `failed` int(10) unsigned NOT NULL DEFAULT '0',
  `modified` int(10) unsigned NOT NULL DEFAULT '0',
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fcm_batches_created_at_index` (`created_at`),
  FULLTEXT KEY `search` (`raw_payload`)
) ENGINE=InnoDB AUTO_INCREMENT=2501855 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


insert into `fcm_batches`(`raw_payload`, `created_at`)
values('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque vulputate nisl sapien, vel posuere ex pulvinar ut. Etiam et enim magna. In dignissim venenatis vulputate. Morbi volutpat purus lectus. Sed tristique ligula ultricies, interdum est eu, condimentum velit. Interdum et malesuada fames ac ante ipsum primis in faucibus. Pellentesque laoreet tempus euismod. In sit amet suscipit nulla. Praesent tempor in libero in molestie. Nulla hendrerit hendrerit scelerisque. Mauris congue lacus ut metus faucibus, sit amet rutrum enim consequat. Curabitur pulvinar vehicula nulla. Aenean condimentum ligula id dolor volutpat, vel porta diam tincidunt. Cras porttitor neque ante. Aenean tellus nisi, finibus ut scelerisque tincidunt, blandit sit amet erat.

Nam luctus enim eros, luctus tincidunt magna porta eu. Praesent ac ligula in sem hendrerit porttitor. Donec vitae dui tincidunt, tempus enim ac, vulputate turpis. Proin convallis accumsan euismod. Integer sed eros diam. Aliquam tincidunt, turpis vel vulputate blandit, lectus quam lacinia mauris, et tempus arcu est nec urna. Aenean porttitor velit at diam volutpat auctor. Duis eu cursus nunc. Vivamus a sapien dictum, posuere neque vitae, laoreet nibh. Nunc at pharetra purus. Sed massa sapien, convallis vitae sem non, ornare hendrerit risus. Nunc at molestie leo, nec sodales eros.

Donec tincidunt dui nec nibh fringilla vehicula. Curabitur egestas, nisi gravida tristique consequat, elit odio pretium eros, eu porta est erat sit amet tortor. Morbi interdum, arcu in ultricies tincidunt, dolor risus consectetur felis, at bibendum sem arcu vitae metus. Suspendisse eu ex dolor. Proin vitae porttitor odio. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Sed volutpat, nibh sit amet volutpat molestie, dolor turpis laoreet diam, faucibus posuere tellus lectus in mauris. Aliquam vestibulum urna at augue molestie blandit. Maecenas vitae dolor metus. Curabitur tempus odio efficitur risus dapibus ullamcorper. Integer et mauris mollis enim ullamcorper lacinia. Nunc dapibus pulvinar arcu, in blandit quam tempus vel. Morbi congue mauris eros, vel efficitur purus efficitur non.

Nunc in semper orci, a elementum mauris. Quisque vulputate volutpat eleifend. Etiam vel rutrum tellus, a varius magna. Donec et lectus tempor, elementum elit in, congue elit. Aliquam eget diam vulputate, mollis tellus et, ornare massa. Donec accumsan pretium arcu, fringilla fermentum odio. Suspendisse in velit metus. Pellentesque eget sagittis felis. Nulla laoreet lacus quis rhoncus sodales. Quisque eleifend posuere orci, a euismod dui egestas sit amet. Vestibulum et felis sapien. Cras imperdiet urna id magna hendrerit, sed pharetra massa dapibus. Praesent at tincidunt ipsum, non dapibus metus. Vestibulum non felis eget nulla vulputate commodo ac a nisl.

Morbi auctor urna nec est vulputate, eu aliquam sem efficitur. Phasellus blandit odio at leo venenatis tempor sed quis lacus. Nunc vel fringilla augue, ac tincidunt nisl. Mauris ut mi eu nibh semper cursus. Nulla facilisi. Nullam commodo placerat enim, eu consectetur turpis imperdiet maximus. Vestibulum efficitur turpis sed porta cursus. Aliquam ut mattis nisi. Donec ut vestibulum ligula. Duis nec libero libero. In consectetur tincidunt rutrum.

Curabitur fermentum rutrum mauris. Cras vestibulum odio ornare ante consequat aliquet. Suspendisse elit lorem, gravida non pellentesque sed, faucibus vitae dolor. Etiam dignissim dolor sit amet turpis ornare, sit amet cursus justo fermentum. In eleifend nec nulla a aliquet. Phasellus convallis magna at mauris imperdiet, vitae mattis leo pharetra. In eu rhoncus libero. Aenean non varius nisi, sit amet luctus dui. Aliquam id nisl in urna pellentesque dapibus ut consectetur augue. Ut in risus vestibulum ex sagittis fermentum eget sed nisi. Sed tincidunt suscipit dui, ut condimentum leo elementum sit amet. Morbi eleifend risus sagittis dui tristique, in porta risus mattis. Fusce vestibulum nisl sed ipsum mattis, sed laoreet lorem accumsan. Nulla egestas turpis in eleifend tincidunt.

Vestibulum lobortis molestie ligula sit amet congue. Nam justo erat, dictum sed vulputate ac, tincidunt non libero. Nunc et feugiat metus, et pulvinar neque. Fusce elementum sapien eu ipsum rhoncus, quis interdum ex faucibus. Fusce a fringilla est. Ut sit amet quam justo. Nulla hendrerit erat leo. Vestibulum a tortor non elit commodo pulvinar id in neque. Nulla nisi dolor, aliquet nec nulla sed, volutpat accumsan elit. Quisque vitae sodales felis, vel eleifend neque. Sed scelerisque tortor risus, nec gravida purus lobortis vel.

Etiam at augue facilisis felis posuere ultrices. In tempus risus eget tempor dignissim. In tortor lectus, viverra quis iaculis sit amet, scelerisque non felis. In enim quam, faucibus et vulputate eu, viverra sed risus. Nunc semper eros vitae nulla molestie gravida. Integer eleifend lorem in ligula imperdiet, mattis vehicula sapien ornare. Maecenas vel ligula vitae ex consequat semper in vel ipsum. Etiam sed elementum justo. Aenean venenatis urna malesuada, sollicitudin mi quis, gravida urna. Nunc a tellus sit amet urna lobortis dictum. Pellentesque euismod quis ipsum id pulvinar. Etiam ultrices, sem ut ullamcorper congue, nulla mauris pellentesque turpis, vitae congue turpis justo vel magna. Cras vitae augue justo.

In hac habitasse platea dictumst. Donec faucibus consectetur bibendum. Donec vitae lectus eget felis congue lacinia vitae vel nulla. Proin mattis elit sed sem rutrum, at varius massa convallis. Donec lacus turpis, placerat non aliquam eget, accumsan quis mi. Nulla at nunc lorem. Nulla congue, turpis eget volutpat varius, leo mi dignissim quam, ut sollicitudin sem metus a lorem. Quisque elementum diam lacus, et imperdiet lectus luctus egestas. Praesent rhoncus lorem massa, non auctor tortor cursus id.

Mauris porta, erat ac condimentum hendrerit, lorem turpis aliquam libero, et pharetra arcu risus a turpis. Sed aliquam at dolor eu sollicitudin. Sed non ligula arcu. Praesent eu est egestas, tempus elit aliquam, elementum ex. Nam hendrerit leo eget eros tempor, ac pellentesque enim tempus. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nunc venenatis magna non ultrices imperdiet. Quisque malesuada dui tincidunt mauris sagittis suscipit. Ut maximus tellus nisi, eu faucibus nisi scelerisque quis. Sed at elementum odio, ac maximus nunc. Quisque non semper nunc. Integer fermentum cursus luctus. Cras facilisis, eros a cursus gravida, felis lectus sagittis sem, eget euismod neque lectus id nibh.

Fusce dapibus nisi sed turpis bibendum posuere. Etiam neque elit, fringilla a nisl sed, tempor tempor ipsum. Fusce in nisi quis nibh consequat vestibulum. In hac habitasse platea dictumst. Suspendisse potenti. Nunc tempus diam quis lacus ullamcorper pretium. Vestibulum a maximus massa, quis euismod turpis. Integer volutpat, dui ac scelerisque molestie, metus tellus facilisis nunc, ut hendrerit arcu diam sit amet augue. Pellentesque vitae magna vel sapien ornare auctor. Maecenas molestie ultrices euismod. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Pellentesque efficitur mi ac velit finibus elementum. Sed vestibulum vel dolor id efficitur. Aenean eleifend neque arcu, vitae vehicula enim rutrum vitae. Donec iaculis maximus aliquam. Sed faucibus viverra lorem vitae malesuada.

Donec aliquet elit magna, ac finibus nisi scelerisque eget. Proin ullamcorper turpis a tempus pulvinar. Vivamus vel ante semper, dictum dui eu, ultricies est. Aenean congue scelerisque lorem, in fermentum nulla interdum eu. Nunc volutpat tempor quam sit amet interdum. Duis nec ligula ut lacus tristique pulvinar ac nec ante. Nulla sagittis, justo et euismod laoreet, enim ligula laoreet enim, id feugiat justo purus id nulla. Nam tempus nisl et dapibus pellentesque. Praesent ac justo non lectus vehicula aliquam vel non odio. Cras nec facilisis odio, vel consequat sapien. In hac habitasse platea dictumst.

Vestibulum non neque non sem interdum facilisis. Pellentesque et gravida nisl. Sed malesuada, ligula non euismod faucibus, felis lectus consequat odio, iaculis commodo metus lorem a urna. Nam eu orci non nibh egestas rhoncus. Praesent massa neque, congue aliquet neque ac, consectetur pulvinar augue. In sagittis commodo nunc, eu feugiat enim aliquam a. Nullam tristique gravida est, non iaculis magna accumsan vitae. Nunc pulvinar, velit eget pharetra lobortis, sem mauris aliquam nisl, sed rutrum sem nibh id urna. Morbi sit amet pulvinar augue. Curabitur ac pretium lorem. Sed eu euismod sem. Nunc ac tortor semper, sollicitudin odio a, efficitur purus.

Morbi sit amet efficitur lorem. Nam urna libero, varius non consectetur nec, molestie quis ex. Duis eu tincidunt nibh. Aenean dictum felis at gravida tristique. Donec nulla tortor, faucibus et tincidunt ut, dapibus eget magna. Mauris et hendrerit quam. Duis a condimentum libero, id imperdiet nunc. Sed ut lacus mi. Phasellus mollis sem arcu. Vestibulum sodales nunc et velit accumsan bibendum. Suspendisse in neque odio. Fusce vitae nulla non nisi bibendum dignissim id vel odio. Duis auctor lacus urna, vel tincidunt nisl porttitor et.

Curabitur iaculis eu neque sed suscipit. Sed ornare, sem vitae dapibus tincidunt, orci dui ultrices diam, id consectetur ante nisi quis ipsum. Aliquam a elit magna. Curabitur eleifend nunc a arcu venenatis, eu elementum odio suscipit. Mauris id sem rhoncus leo luctus euismod. Aenean sit amet orci eu erat accumsan porttitor efficitur dictum erat. Pellentesque eu tempor ante. Vestibulum turpis nisi, venenatis semper justo sit amet, finibus convallis augue. Donec iaculis tempor fringilla. Aenean ligula libero, scelerisque in mauris quis, mollis venenatis magna. Integer aliquam odio a augue aliquet efficitur.

Morbi sapien ipsum, tristique at est non, finibus volutpat metus. Praesent dapibus eros vel augue condimentum fermentum. Aenean et mattis leo, non mattis lacus. Quisque venenatis a massa sed luctus. Quisque fringilla ultrices nisl nec gravida. Nullam euismod scelerisque luctus. Cras imperdiet euismod dolor porta auctor. Nulla feugiat sagittis sagittis. Vestibulum et orci neque. Donec sit amet dictum massa. Fusce ultrices quam nibh, vel suscipit sapien gravida eu.

Nulla placerat tellus nec ornare mattis. Vivamus ut gravida nisi. Aliquam erat volutpat. Proin porttitor lectus ac libero mattis porta. Aenean nec congue lectus, et imperdiet leo. Quisque vitae tristique elit. Aliquam magna ante, imperdiet sit amet tempus vitae, bibendum sit amet quam. Ut egestas convallis nibh, eu pulvinar nisl porttitor ac. Nulla nec porttitor ante. Aliquam luctus pretium nisl in euismod. Vivamus a felis vel ex tempor viverra. Vivamus at sapien vel purus hendrerit luctus.

Nullam vulputate, turpis ut molestie tempus, diam est porttitor elit, quis vestibulum lectus lacus et tellus. Mauris odio ligula, suscipit ac mi quis, volutpat luctus sapien. Vivamus eros mauris, aliquam eget efficitur scelerisque, condimentum iaculis diam. Maecenas eu odio lectus. Sed rutrum leo sit amet tincidunt blandit. Etiam quis urna tincidunt, feugiat sem nec, facilisis odio. Donec hendrerit nisl quis quam pellentesque, quis lobortis eros interdum. Nulla quis rutrum arcu. Morbi hendrerit mauris nibh, sed pellentesque dolor commodo at. Etiam rhoncus justo non purus vestibulum, at lobortis leo tincidunt. Integer fermentum ligula eget facilisis porta. Donec cursus congue luctus. Suspendisse a sodales ligula. Nunc blandit purus nec faucibus finibus. Nunc iaculis, ex ac mattis tempor, quam tortor ultricies nibh, a luctus quam ipsum varius diam.

Sed non eleifend nisi, eu interdum leo. Morbi congue, eros a luctus fermentum, libero mauris imperdiet eros, accumsan tempor metus felis nec ligula. Duis posuere a ex sit amet commodo. Donec rhoncus magna ut massa congue facilisis. Vivamus volutpat nisi vitae est pulvinar, dictum sollicitudin elit sodales. Nulla facilisi. Phasellus placerat lorem urna, sed pellentesque ipsum convallis eget. Fusce purus ipsum, condimentum a neque eget, sagittis dignissim justo. Integer et turpis massa. Etiam at elementum nibh. Maecenas maximus quis erat non tempus. Donec consequat scelerisque ornare. Integer vulputate neque et ipsum tincidunt pharetra. Nulla sit amet interdum nisi, eget tristique lorem.

Donec eleifend, purus et posuere bibendum, lorem mi laoreet arcu, nec tincidunt ex ipsum nec velit. Praesent turpis turpis, finibus quis est quis, tempor ullamcorper nunc. Phasellus pretium dui a posuere finibus. Nunc pretium erat ut erat fermentum, non ultrices nunc mollis. Curabitur suscipit sit amet massa ut tristique. Interdum et malesuada fames ac ante ipsum primis in faucibus. Etiam lobortis nisi sed urna hendrerit cursus. Nulla facilisi. Aliquam a consequat tortor, a pulvinar arcu. Vivamus sit amet ex vitae erat aliquam aliquam. Duis aliquet orci mauris, quis dictum tortor convallis sit amet. Maecenas viverra tincidunt placerat.

Maecenas felis ex, lobortis eu sapien ac, sollicitudin laoreet ligula. Morbi id scelerisque sem, eget viverra nulla. Fusce tincidunt lacinia neque ac lacinia. Fusce vitae dapibus tellus. Vestibulum at sollicitudin mi, ac condimentum lectus. Sed at nisi eu nibh scelerisque porta. Cras nunc nisl, consequat eget feugiat sed, eleifend sed augue. Aliquam malesuada venenatis metus eu viverra. Praesent nec blandit nisi. Ut in nibh vel ex posuere euismod.

Pellentesque ac lorem rutrum eros iaculis pharetra sed eu nibh. Pellentesque sagittis nisi massa, in iaculis diam suscipit non. Nullam pretium, mauris vitae dictum convallis, nulla libero consequat enim, ac feugiat eros ipsum et lorem. Ut ut sem ultrices, euismod felis rutrum, semper lacus. Nullam commodo interdum augue, in consectetur turpis pharetra ut. Pellentesque scelerisque mi eget velit pharetra porta. In hac habitasse platea dictumst. Curabitur elit dui, convallis quis dolor eu, pulvinar elementum nibh. Maecenas eu orci eget neque lobortis volutpat. Vestibulum feugiat arcu id tellus efficitur iaculis. In nec ligula ac elit sodales sagittis ac a nunc. Aenean convallis tortor ac risus iaculis, vel mollis augue scelerisque. Pellentesque placerat commodo massa id mattis.

Donec condimentum, purus eu convallis dictum, nunc nisl eleifend diam, vel luctus neque lorem ac eros. Fusce varius nulla sit amet lobortis pulvinar. Proin fermentum magna et ipsum gravida, vel sollicitudin neque suscipit. Mauris scelerisque at nisi non efficitur. Mauris fringilla imperdiet nisl et laoreet. Maecenas dictum feugiat finibus. Maecenas dignissim purus felis, at scelerisque neque aliquet in. Sed ac dapibus ipsum, egestas sagittis enim. Aenean feugiat quis enim ut fermentum. Nullam elementum blandit est, eget sagittis ligula porttitor id.

Ut rhoncus, tortor sed venenatis consectetur, eros lorem tempor enim, et dapibus nunc nisl a odio. Suspendisse ac lacinia diam. Nullam accumsan, quam at tincidunt elementum, mauris augue convallis neque, quis sagittis ante nulla at nunc. Sed eu egestas quam. Sed in ante suscipit, viverra nulla id, dignissim mauris. Maecenas velit lectus, mollis in sapien at, luctus ultricies ante. Sed fermentum nunc vitae dictum varius. Nam lobortis nunc at nulla aliquet, vel iaculis mauris consectetur. Phasellus quis pharetra arcu, a tincidunt ex. Donec lacinia tincidunt elit, non sodales orci consequat sit amet. Aliquam mollis nibh et tortor ultricies varius. Sed vestibulum orci eu ligula tincidunt fringilla quis ac lorem. Maecenas vehicula consectetur erat at sollicitudin.

Aliquam erat volutpat. Vestibulum ornare turpis lobortis, faucibus dolor tincidunt, dapibus libero. Etiam consectetur pellentesque lobortis. Integer mollis mi at nibh facilisis tincidunt. Nullam a convallis risus, finibus volutpat eros. Nulla ut venenatis nibh. Nullam porttitor lectus risus, at efficitur nibh tempus nec. Duis ac enim non augue hendrerit tristique ut sed arcu. Fusce cursus sodales sapien non pretium. Curabitur vitae quam sed odio faucibus ultrices. Quisque in malesuada ex, vitae sodales quam. Morbi non libero lectus. Quisque porttitor semper egestas.

Nulla ac posuere sem. Aenean congue, odio at molestie vestibulum, orci dolor imperdiet ex, non suscipit dolor nulla eu ante. Sed aliquam tellus nisi, in auctor enim finibus vitae. Morbi ornare non urna eget rutrum. In hendrerit egestas felis eget dapibus. Fusce vehicula quam sed dolor tincidunt pretium. Cras dapibus magna at mi tristique luctus. Maecenas laoreet luctus est porttitor finibus. Sed viverra eget sapien ac lobortis. Donec eu faucibus nibh, eu tincidunt augue. Aenean imperdiet, massa eget sollicitudin lobortis, dolor nisi efficitur lorem, quis feugiat eros ex quis est.

Duis hendrerit, dui vel bibendum placerat, mi arcu finibus lacus, vitae porttitor orci neque sit amet augue. Sed felis nulla, fringilla at urna sed, varius ullamcorper leo. Nam in vehicula enim, ut tincidunt tortor. Fusce ac lorem ullamcorper, ultrices urna nec, volutpat mauris. Cras tempus nunc eu cursus semper. Proin mollis quam et nisi placerat, a viverra leo cursus. Nulla ut consequat risus. Morbi pellentesque risus vel scelerisque rutrum.

Vestibulum hendrerit, orci ac vulputate convallis, eros orci sodales sapien, id aliquam arcu ligula nec nisi. Donec at magna eu eros posuere efficitur. In vestibulum dignissim ante, at luctus neque. Donec sit amet nisi vitae ex porta sollicitudin. Aliquam erat volutpat. Curabitur molestie pharetra leo, a rhoncus urna convallis ac. Nam ut ex vitae purus blandit tempus vitae eu ipsum. Nullam pellentesque, nunc sit amet mattis venenatis, lacus arcu cursus ante, nec porta turpis neque a augue. Cras a erat auctor, gravida nulla sed, tincidunt urna.

Praesent ut pharetra neque, ut interdum mauris. Donec quis laoreet elit, vel cursus sapien. Vestibulum quis nunc auctor, hendrerit arcu vitae, ullamcorper dui. Sed ac accumsan mi. Vestibulum lorem diam, molestie eget lobortis vel, finibus ac augue. In maximus ornare porttitor. Morbi non diam et nisi ornare vestibulum. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Quisque dictum urna ac arcu hendrerit fermentum. Suspendisse consectetur enim quis lectus ultricies iaculis. Pellentesque congue in orci a efficitur. Fusce volutpat libero massa, a aliquam ligula finibus sed. In dapibus nunc et lobortis tempus.

Nulla et lacus feugiat, efficitur massa sit amet, vulputate purus. Cras fringilla rhoncus nibh at fermentum. Nulla facilisi. Ut efficitur, nunc ac finibus blandit, tortor augue rhoncus nibh, vitae cursus tortor ipsum tristique augue. Phasellus lacinia eu est sit amet ultricies. Phasellus gravida varius ipsum, sed dictum neque facilisis at. Vivamus hendrerit, nibh in hendrerit iaculis, est risus tincidunt lorem, eu volutpat diam ipsum sed dolor. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Interdum et malesuada fames ac ante ipsum primis in faucibus. Vivamus lacinia metus et arcu scelerisque, vitae imperdiet tellus luctus. Donec ut aliquam turpis, ut dapibus ligula. Donec quis hendrerit risus, in sagittis tellus. Quisque ante sapien, condimentum id pretium eget, malesuada sed ex. Vestibulum cursus fermentum sem vitae pellentesque.

Morbi viverra sed lacus eget pellentesque. Quisque quis massa ac magna commodo molestie vel et lorem. Phasellus metus ligula, faucibus ac interdum eget, vulputate sed tellus. Donec id mi et felis finibus pulvinar id eu massa. Donec aliquam sem a nisi lobortis consequat. Proin bibendum laoreet risus, eget hendrerit felis faucibus sit amet. Sed quis ultricies sapien. Maecenas at tincidunt turpis. Ut fringilla rutrum nisi, eu pharetra turpis hendrerit at. Vivamus non nisl id justo feugiat interdum. Nam feugiat, justo ut vestibulum lacinia, enim massa varius risus, quis feugiat erat justo id erat.

Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Aliquam sit amet augue dignissim, tincidunt massa a, efficitur dui. Sed bibendum vel justo sed lacinia. Vivamus velit ex, consequat nec volutpat eget, sollicitudin id ligula. Aenean vitae ipsum et mauris dignissim pharetra. Vivamus dapibus lectus sit amet mauris blandit volutpat. Praesent diam leo, euismod nec felis sed, lacinia molestie sem. Fusce sed sodales leo, et condimentum ante. In ac ipsum at nulla dapibus ornare. Sed egestas dictum dui, id ultricies velit semper ut. Curabitur vel neque id nisl consequat lacinia. Duis ullamcorper, quam a rhoncus mollis, orci tellus maximus nibh, ac aliquet est purus id erat. Sed accumsan porta risus, nec blandit libero cursus et. Suspendisse auctor tortor non ipsum bibendum, non volutpat tellus mattis. Etiam gravida nisl dolor, vel euismod sapien tincidunt a. Donec commodo placerat urna sit amet ullamcorper.

Nullam convallis nec justo in ornare. Etiam ac placerat est. Mauris rhoncus maximus metus, vitae elementum erat tempus ut. Duis at porta orci, id aliquam neque. Fusce semper augue diam, sed posuere magna volutpat at. Proin hendrerit quam eu sapien mollis dignissim. Integer consectetur rhoncus est, et posuere odio mattis facilisis. Donec sed elit non ligula semper varius eget ut tellus. Proin sed dolor et dui volutpat dapibus et eu nulla. In in libero non elit tempor venenatis et eget magna. Mauris tellus ipsum, semper id velit ac, posuere auctor felis.

In hac habitasse platea dictumst. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla in facilisis metus, ut condimentum augue. Cras lobortis libero eu elit fermentum iaculis. Morbi ut mattis odio, ac consequat mi. Pellentesque pretium ornare nunc id commodo. Vivamus et nibh lorem. Phasellus placerat ligula massa, eget tristique tellus luctus sed. Morbi et vulputate augue. Duis sem tellus, finibus a euismod at, fermentum ac lacus. Integer elementum ut tortor et porta. Donec interdum metus sem, vitae vestibulum velit rhoncus id. Morbi elit dolor, eleifend at sodales eu, euismod sit amet est.

Duis euismod quis est in dictum. Sed lacinia posuere nisi, in bibendum eros semper sed. Praesent luctus mattis leo a porttitor. Aenean molestie id lectus lacinia tincidunt. Proin egestas non est sit amet aliquam. Sed blandit libero at lacinia aliquam. Aenean sollicitudin metus vel ullamcorper fermentum. Integer vestibulum eget tellus vel interdum. Phasellus viverra elit quis semper aliquet. Integer efficitur augue neque, vel vehicula quam egestas nec. Sed dui lectus, porttitor quis consequat vel, feugiat non justo. Morbi eget scelerisque lacus.', now());
