package com.palmap.rssi.common

object Common {

    val MACHINE_SET_FILE = "machine_list"
    val ZK_MACHINE_SET = "machineSet"
    val MAC_BRAND = "mac_brand"
    val MACHINE_BRAND_SET_FILE = "machineBrand_list"
    val MACHINE_SET_PATH = "/com/palmaplus/rssi/conf/machine_new"
    val SCENE_ID_MAP = "sceneIdMap"

    val BRAND_UNKNOWN = "unknown"
    val SHOP_SCENE_IDS_URL = "sceneIdsUrl"

    val CUSTOMER_JUDGE = 5
    val MAC_KEY_LENGTH = 8

    val CTRL_A = "\u0001"

    val INTERVAL_MINUTE = 10  //分钟
    val MINUTE_FORMATTER = 60000
    val DEFAULT_MACHINE_CHECK_MINUTE = 2 * 5
    val NOW_MINUTE_FORMAT = "yyyy-MM-dd HH:mm:00"
    val NOW_HOUR_FORMAT = "yyyy-MM-dd HH:00:00"
    val TODAY_FIRST_TS_FORMAT = "yyyy-MM-dd 00:00:00"
    val HOUR_FORMATTER = MINUTE_FORMATTER * 60
    val DAY_FORMATTER = MINUTE_FORMATTER * 60 * 24
    val DEFAULT_MACHINE_CHECK_TIMES = 1

    val ZK_MAP_MONITOR_PATH = "zk.map.monitor.path"
    val ZOOKEEPER_QUORUM = "zkQuorum"
    val SCENE_ID_MONITOR_PATH = "sceneid.monitor.path"

    val SPARK_CONFIG = "sparkRssiInfo.xml"
    val KAFKA_METADATA_BROKER = "metadata.broker.list"
    val SPARK_GROUP_ID = "group.id"
    val SPARK_TOPICS = "topics"

    val MONGO_DB_NAME = "mongo.db.name"
    val MONGO_ADDRESS_LIST = "mongo.address.list"
    val MONGO_SERVER_PORT = "mongo.server.port"
    val MONGO_SERVER_CONNECTIONS_PER_HOST = "mongo.server.connectionsPerHost"
    val MONGO_SERVER_AUTHENTICATE = "mongo.server.authenticate"
    val MONGO_SERVER_THREADS = "mongo.server.threads"
    val MONGO_SERVER_USER = "mongo.server.user"
    val MONGO_SERVER_PWD = "mongo.server.pwd"

    val STORE_BUSINESS_HOURS = "storeBusinessHour"

    val MONGO_OPTION_ID = "_id"
    val MONGO_OPTION_GTE = "$gte"
    val MONGO_OPTION_INC = "$inc"
    val MONGO_OPTION_SET = "$set"
    val MONGO_OPTION_SLICE = "$slice"
    val MONGO_OPTION_EACH = "$each"
    val MONGO_OPTION_ADD_TO_SET = "$addToSet"
    val MONGO_OPTION_IN = "$in"

    val MONGO_COLLECTION_SHOP_HISTORY = "shop_history"
    val MONGO_HISTORY_SHOP_SCENE_ID = "sceneId"
    val MONGO_HISTORY_SHOP_MAC = "mac"
    val MONGO_HISTORY_SHOP_TIMES = "times"
    val MONGO_HISTORY_SHOP_FIRST_DATE = "firstDate"
    val MONGO_HISTORY_SHOP_LAST_DATE = "lastDate"
    val MONGO_HISTORY_SHOP_DAYS = "days"

    val MONGO_COLLECTION_SHOP_DAY_INFO = "shop_day_info"
    val MONGO_HISTORY_SHOP_DAY_INFO_SCENE_ID = "sceneId"
    val MONGO_HISTORY_SHOP_DAY_INFO_DATE = "date"
    val MONGO_HISTORY_SHOP_DAY_INFO_COUNT = "count"
    val MONGO_HISTORY_SHOP_DAY_INFO_DWELL = "dwell"

    val MONGO_COLLECTION_SHOP_VISITED = "shop_visited"
    val MONGO_SHOP_VISITED_DATE = "date"
    val MONGO_SHOP_VISITED_SCENE_ID = "sceneId"
    val MONGO_SHOP_VISITED_MAC = "mac"
    val MONGO_SHOP_VISITED_DWELL = "dwell"
    val MONGO_SHOP_VISITED_TIMES = "times"
    val MONGO_SHOP_VISITED_FREQUENCY = "frequency"
    val MONGO_SHOP_VISITED_IS_CUSTOMER = "isCustomer"
    val MONGO_SHOP_VISITED_PHONE_BRAND = "brand"

    val MONGO_COLLECTION_SHOP_REAL_TIME = "shop_realtime"
    val MONGO_SHOP_REAL_TIME_SCENE_ID = "sceneId"
    val MONGO_SHOP_REAL_TIME_TIME = "time"
    val MONGO_SHOP_REAL_TIME_MAC_SUM = "macSum"
    val MONGO_SHOP_REAL_TIME_MACS = "macs"
    val MONGO_SHOP_REAL_TIME_IS_CUSTOMER = "isCustomer"

    val MONGO_COLLECTION_SHOP_REAL_TIME_HOUR = "shop_realtime_hour"
    val MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID = "sceneId"
    val MONGO_SHOP_REAL_TIME_HOUR = "hour"
    val MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER = "isCustomer"
    val MONGO_SHOP_REAL_TIME_HOUR_MACS = "macs"

    val MONGO_COLLECTION_SHOP_CONNECTIONS = "shop_connections"
    val MONGO_SHOP_CONNECTIONS_SCENE_ID = "sceneId"
    val MONGO_SHOP_CONNECTIONS_TIME = "time"
    val MONGO_SHOP_CONNECTIONS_IS_CUSTOMER = "isCustomer"
    val MONGO_SHOP_CONNECTIONS_MACS = "macs"
    val MONGO_SHOP_CONNECTIONS_MAC_SUM = "macSum"

    val MONGO_COLLECTION_SHOP_STATIC_INFO = "shop_static_info"
    val MONGO_STATIC_INFO_SHOP_SCENE_ID = "sceneId"
    val MONGO_STATIC_INFO_SHOP_OPEN_MINUTE = "openMinute"
    val MONGO_STATIC_INFO_SHOP_CLOSE_MINUTE = "closeMinute"

    val SCENE_ID_HUAWEI = 13519  // 华为的sceneId 统计每分钟连接数用

}
