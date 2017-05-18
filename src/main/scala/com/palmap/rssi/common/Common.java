/**
 * @author caoyupeng@doodod.com
 */
package com.palmap.rssi.common;

public class Common {

    public static final String MACHINE_SET_FILE = "machine_list";
    public static final String ZK_MACHINE_SET = "machineSet";
    public static final String MAC_BRAND = "mac_brand";
    public static final String MACHINE_BRAND_SET_FILE = "machineBrand_list";
    public static final String MACHINE_SET_PATH = "/com/palmaplus/rssi/conf/machine_new";
    public static final String SCENE_ID_MAP = "sceneIdMap";


    public static final String BRAND_UNKNOWN = "unknown";
    public static final String SHOP_SCENE_IDS_URL = "sceneIdsUrl";

    public static final int CUSTOMER_JUDGE = 5;
    public static final int MAC_KEY_LENGTH = 8;

    public static final String CTRL_A = "\u0001";

    public static final int INTERVAL_MINUTE = 10;  //分钟
    public static final int MINUTE_FORMATTER = 60000;
    public static final int DEFAULT_MACHINE_CHECK_MINUTE = 2 * 5;
    public static final String NOW_MINUTE_FORMAT = "yyyy-MM-dd HH:mm:00";
    public static final String NOW_HOUR_FORMAT = "yyyy-MM-dd HH:00:00";
    public static final String TODAY_FIRST_TS_FORMAT = "yyyy-MM-dd 00:00:00";
    public static final int HOUR_FORMATTER = MINUTE_FORMATTER * 60;
    public static final int DAY_FORMATTER = MINUTE_FORMATTER * 60 * 24;
    public static final int DEFAULT_MACHINE_CHECK_TIMES = 1;

    public static final String ZK_MAP_MONITOR_PATH = "zk.map.monitor.path";
    public static final String ZOOKEEPER_QUORUM = "zkQuorum";
    public static final String SCENE_ID_MONITOR_PATH = "sceneid.monitor.path";

    public static final String SPARK_CONFIG = "sparkRssiInfo.xml";
    public static final String KAFKA_METADATA_BROKER = "metadata.broker.list";
    public static final String SPARK_GROUP_ID = "group.id";
    public static final String SPARK_TOPICS = "topics";

    public static final String MONGO_DB_NAME = "mongo.db.name";
    public static final String MONGO_ADDRESS_LIST = "mongo.address.list";
    public static final String MONGO_SERVER_PORT = "mongo.server.port";
    public static final String MONGO_SERVER_CONNECTIONS_PER_HOST = "mongo.server.connectionsPerHost";
    public static final String MONGO_SERVER_AUTHENTICATE = "mongo.server.authenticate";
    public static final String MONGO_SERVER_THREADS = "mongo.server.threads";
    public static final String MONGO_SERVER_USER = "mongo.server.user";
    public static final String MONGO_SERVER_PWD = "mongo.server.pwd";

    public static final String STORE_BUSINESS_HOURS = "storeBusinessHour";

    public static final String MONGO_OPTION_ID = "_id";
    public static final String MONGO_OPTION_GTE = "$gte";
    public static final String MONGO_OPTION_INC = "$inc";
    public static final String MONGO_OPTION_SET = "$set";
    public static final String MONGO_OPTION_SLICE = "$slice";
    public static final String MONGO_OPTION_EACH = "$each";
    public static final String MONGO_OPTION_ADD_TO_SET = "$addToSet";
    public static final String MONGO_OPTION_IN = "$in";

    public static final String MONGO_COLLECTION_SHOP_HISTORY = "shop_history";
    public static final String MONGO_HISTORY_SHOP_SCENE_ID = "sceneId";
    public static final String MONGO_HISTORY_SHOP_MAC = "mac";
    public static final String MONGO_HISTORY_SHOP_TIMES = "times";
    public static final String MONGO_HISTORY_SHOP_FIRST_DATE = "firstDate";
    public static final String MONGO_HISTORY_SHOP_LAST_DATE = "lastDate";
    public static final String MONGO_HISTORY_SHOP_DAYS = "days";

    public static final String MONGO_COLLECTION_SHOP_DAY_INFO = "shop_day_info";
    public static final String MONGO_HISTORY_SHOP_DAY_INFO_SCENE_ID = "sceneId";
    public static final String MONGO_HISTORY_SHOP_DAY_INFO_DATE = "date";
    public static final String MONGO_HISTORY_SHOP_DAY_INFO_COUNT = "count";
    public static final String MONGO_HISTORY_SHOP_DAY_INFO_DWELL = "dwell";

    public static final String MONGO_COLLECTION_SHOP_VISITED = "shop_visited";
    public static final String MONGO_SHOP_VISITED_DATE = "date";
    public static final String MONGO_SHOP_VISITED_SCENE_ID = "sceneId";
    public static final String MONGO_SHOP_VISITED_MAC = "mac";
    public static final String MONGO_SHOP_VISITED_DWELL = "dwell";
    public static final String MONGO_SHOP_VISITED_TIMES = "times";
    public static final String MONGO_SHOP_VISITED_FREQUENCY = "frequency";
    public static final String MONGO_SHOP_VISITED_IS_CUSTOMER = "isCustomer";
    public static final String MONGO_SHOP_VISITED_PHONE_BRAND = "brand";

    public static final String MONGO_COLLECTION_SHOP_REAL_TIME = "shop_realtime";
    public static final String MONGO_SHOP_REAL_TIME_SCENE_ID = "sceneId";
    public static final String MONGO_SHOP_REAL_TIME_TIME = "time";
    public static final String MONGO_SHOP_REAL_TIME_MAC_SUM = "macSum";
    public static final String MONGO_SHOP_REAL_TIME_MACS = "macs";
    public static final String MONGO_SHOP_REAL_TIME_IS_CUSTOMER = "isCustomer";

    public static final String MONGO_COLLECTION_SHOP_REAL_TIME_HOUR = "shop_realtime_hour";
    public static final String MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID = "sceneId";
    public static final String MONGO_SHOP_REAL_TIME_HOUR = "hour";
    public static final String MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER = "isCustomer";
    public static final String MONGO_SHOP_REAL_TIME_HOUR_MACS = "macs";

    public static final String MONGO_COLLECTION_SHOP_STATIC_INFO = "shop_static_info";
    public static final String MONGO_STATIC_INFO_SHOP_SCENE_ID = "sceneId";
    public static final String MONGO_STATIC_INFO_SHOP_OPEN_MINUTE = "openMinute";
    public static final String MONGO_STATIC_INFO_SHOP_CLOSE_MINUTE = "closeMinute";
}
