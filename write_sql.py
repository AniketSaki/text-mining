import mysql.connector
from mysql.connector import errorcode

user = 'aniket'
password = 'aniket'
root = 'admin'
database = 'twitter'
host = '127.0.0.1'

TABLES = {}
TABLES['tweet'] = (
    "CREATE TABLE `tweet` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`actor_id` int(18) NOT NULL,"
    "	`id_str` varchar(40) NOT NULL,"
    "	`objectType` varchar(10) NOT NULL,"
    "	`verb` varchar(5) NOT NULL,"
    "	`postedTime` datetime NOT NULL,"
    "	`link` varchar(150) NOT NULL,"
    "	`body` varchar(140) NOT NULL,"
    "	`favorites` int(7) NOT NULL,"
    "	`retweet` int(7) NOT NULL,"
    "	`filter_lvl` varchar(10),"
    "	`lang` varchar(20),"
    "	`generator_name` varchar(50) NOT NULL,"
    "	`generator_link` varchar(150) NOT NULL,"
    "	`provider_name` varchar(50) NOT NULL,"
    "	`provider_link` varchar(150) NOT NULL,"
    "	`gnip_lang` varchar(10) NOT NULL,"
    "	PRIMARY KEY (`tweet_id`),"
    "	CONSTRAINT `tweet_actor_fk` FOREIGN KEY (`actor_id`)"
    "		REFERENCES `twitter`.`actor` (`actor_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['actor'] = (
    "CREATE TABLE `actor` ("
    "	`actor_id` int(18) NOT NULL,"
    "	`objectType` varchar(10) NOT NULL,"
    "	`link` varchar(150) NOT NULL,"
    "	`displayName` varchar(50) NOT NULL,"
    "	`postedTime` datetime NOT NULL,"
    "	`image` varchar(200),"
    "	`summary` varchar(150),"
    "	`friends` int(7) NOT NULL,"
    "	`followers` int(7) NOT NULL,"
    "	`listed` int(5) NOT NULL,"
    "	`favorites` int(7) NOT NULL,"
    "	`statuses` int(7) NOT NULL,"
    "	`timezone` varchar(50),"
    "	`verified` bool NOT NULL,"
    "	`utcOffset` int(6),"
    "	`preferredName` varchar(50) NOT NULL,"
    "	`id_str` varchar(40) NOT NULL,"
    "	`location` varchar(40),"
    "	PRIMARY KEY (`actor_id`)"
    ") ENGINE=InnoDB")

TABLES['actor_links'] = (
    "CREATE TABLE `actor_links` ("
    "	`actor_id` int(18) NOT NULL,"
    "	`href` varchar(100),"
    "	`rel` varchar(10) NOT NULL,"
    "	CONSTRAINT `actor_link_fk` FOREIGN KEY (`actor_id`)"
    "		REFERENCES `twitter`.`actor` (`actor_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['actor_lang'] = (
    "CREATE TABLE `actor_lang` ("
    "	`actor_id` int(18) NOT NULL,"
    "	`lang` varchar(3),"
    "	CONSTRAINT `actor_lang_fk` FOREIGN KEY (`actor_id`)"
    "		REFERENCES `twitter`.`actor` (`actor_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['gnip_rules'] = (
    "CREATE TABLE `gnip_rules` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`value` varchar(20) NOT NULL,"
    "	`tag` varchar(50),"
    "	CONSTRAINT `tweet_gnip_fk` FOREIGN KEY (`tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['hashtags'] = (
    "CREATE TABLE `hashtags` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`text` varchar(50) NOT NULL,"
    "	`start_index` int(3) NOT NULL,"
    "	`end_index int(3) NOT NULL,"
    "   INDEX `tweet_hashtag_fk_idx` (`tweet_id` ASC),"
    "	CONSTRAINT `tweet_hashtag_fk` FOREIGN KEY ('tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['urls'] = (
    "CREATE TABLE `urls` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`url` varchar(200) NOT NULL,"
    "	`extended_url` varchar(200) NOT NULL,"
    "	`displayUrl` varchar(200) NOT NULL,"
    "	`start_index int(3) NOT NULL,"
    "	`end_index int(3) NOT NULL,"
    "   INDEX `tweet_url_fk_idx` (`tweet_id` ASC),"
    "	CONSTRAINT `tweet_url_fk` FOREIGN KEY ('tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['user_mentions'] = (
    "CREATE TABLE `user_mentions` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`screen_name` varchar(50) NOT NULL,"
    "	`name` varchar(50) NOT NULL,"
    "	`id` int(8) NOT NULL,"
    "	`id_str` varchar(20) NOT NULL,"
    "	`start_index` int(3) NOT NULL,"
    "	`end_index int(3) NOT NULL,"
    "   INDEX `tweet_mention_fk_idx` (`tweet_id` ASC),"
    "	CONSTRAINT `tweet_mention_fk` FOREIGN KEY ('tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['symbols'] = (
    "CREATE TABLE `symbols` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`text` varchar(50) NOT NULL,"
    "	`start_index` int(3) NOT NULL,"
    "	`end_index int(3) NOT NULL,"
    "   INDEX `tweet_symbol_fk_idx` (`tweet_id` ASC),"
    "	CONSTRAINT `tweet_symbol_fk` FOREIGN KEY ('tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['objects'] = (
    "CREATE TABLE `objects` ("
    "	`tweet_id` int(18) NOT NULL,"
    "	`object_id` int(18) NOT NULL,"
    "	`objectType` varchar(10) NOT NULL,"
    "	`summary` varchar(140) NOT NULL,"
    "	`link` varchar(200) NOT NULL,"
    "	`postedTime` datetime NOT NULL,"
    "	`id_str` varchar(200) NOT NULL,"
    "	PRIMARY KEY `object_id`,"
    "   INDEX `tweet_object_fk_idx` (`tweet_id` ASC),"
    "	CONSTRAINT `tweet_object_fk` FOREIGN KEY ('tweet_id`)"
    "		REFERENCES `twitter`.`tweet` (`tweet_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")


def create_db(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database)
        )
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


cnx = mysql.connector.connect(user=user, password=password, host=host)
cursor = cnx.cursor()

try:
    cnx.database = database
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_db(cursor)
        cnx.database = database
    else:
        print(err)
        exit(1)

for name, ddl in TABLES.items():
    try:
        print("Creating table {}: ".format(name), end='')
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists")
        else:
            print(err.msg)
    else:
        print("OK")
cursor.close()
cnx.close()
