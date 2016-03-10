/**
 * @author caoyupeng@doodod.com
 */
package com.palmap.rssi.common;

public class Common {
	public static final String BRAND_UNKNOWN    = "unknown";

	public static final int CUSTOMER_JUDGE = 5;
	public static final int MAC_KEY_LENGTH = 8;

	public static final String CTRL_A  = "\u0001";
	public static final String CTRL_B  = "\u0002";
	public static final String CTRL_C  = "\u0003";

	public static final char BATCH_INTERVAL_IN_MILLI_SEC = 60000;
	public static final int MINUTE_FORMATER  = 60000;
	public static final String NOW_MINUTE_FORMAT = "yyyy-MM-dd HH:mm:00";
	public static final String NOW_HOUR_FORMAT = "yyyy-MM-dd HH:00:00";
	public static final String TODAY_FIRST_TS_FORMAT = "yyyy-MM-dd 00:00:00";

	public static final int CUSTOMER_VALUE = 0;
	public static final int PASSENGER_VALUE = 1;
	public static final int EMPLOYEE_VALUE  = 2;
	public static final int MACHINE_VALUE   = 3;

	public static final String SPARK_CONFIG = "sparkRssiInfo.xml";
	public static final String KAFKA_METADATA_BROKER = "metadata.broker.list";
	public static final String SPARK_GROUP_ID = "group.id";
	public static final String SPARK_TOPICS = "topics";

	public static final String MONGO_DB_NAME = "mongo.db.name";
	public static final String MONGO_ADDRESS_LIST = "mongo.address.list";
	public static final String MONGO_SERVER_PORT= "mongo.server.port";
	public static final String MONGO_SERVER_CONNECTIONSPERHOST = "mongo.server.connectionsPerHost";
	public static final String MONGO_SERVER_AUTHENTICATE = "mongo.server.authenticate";
	public static final String MONGO_SERVER_THREADS = "mongo.server.threads";
	public static final String MONGO_SERVER_USER = "mongo.server.user";
	public static final String MONGO_SERVER_PWD = "mongo.server.pwd";

	public static final String STORE_BUSINESS_HOURS = "storeBusinessHour";

	public static final String MONGO_OPTION_ID = "_id";
	public static final String MONGO_OPTION_PUSH = "$push";
	public static final String MONGO_OPTION_INC = "$inc";
	public static final String MONGO_OPTION_SET  = "$set";
	public static final String MONGO_OPTION_UNSET  = "$unset";
	public static final String MONGO_OPTION_SLICE  = "$slice";
	public static final String MONGO_OPTION_EACH  = "$each";
	public static final String MONGO_OPTION_ALL  = "$all";
	public static final String MONGO_OPTION_SIZE  = "$size";
	public static final String MONGO_OPTION_ADDTOSET  = "$addToSet";

	public static final String MONGO_COLLECTION_SHOP_HISTORY = "shop_history";
	public static final String MONGO_HISTORY_SHOP_SCENEID = "sceneId";
	public static final String MONGO_HISTORY_SHOP_MAC = "mac";
	public static final String MONGO_HISTORY_SHOP_TIMES = "times";
	public static final String MONGO_HISTORY_SHOP_DAYS = "days";

	public static final String MONGO_COLLECTION_SHOP_VISITED = "shop_visited";
	public static final String MONGO_SHOP_VISITED_DATE = "date";
	public static final String MONGO_SHOP_VISITED_SCENEID = "sceneId";
	public static final String MONGO_SHOP_VISITED_MAC = "mac";
	public static final String MONGO_SHOP_VISITED_DWELL = "dwell";
	public static final String MONGO_SHOP_VISITED_TIMES= "times";
	public static final String MONGO_SHOP_VISITED_ISCUSTOMER= "isCustomer";
	public static final String MONGO_SHOP_VISITED_PHONEBRAND="brand";

	public static final String MONGO_COLLECTION_SHOP_REALTIME = "shop_realtime";
	public static final String MONGO_SHOP_REALTIME_SCENEID = "sceneId";
	public static final String MONGO_SHOP_REALTIME_TIME = "time";
	public static final String MONGO_SHOP_REALTIME_MACSUM = "macSum";
	public static final String MONGO_SHOP_REALTIME_MACS = "macs";
	public static final String MONGO_SHOP_REALTIME_ISCUSTOMER= "isCustomer";

	public static final String MONGO_COLLECTION_SHOP_REALTIMEHOUR= "shop_realtime_hour";
	public static final String MONGO_SHOP_REALTIMEHOUR_SCENEID= "sceneId";
	public static final String MONGO_SHOP_REALTIME_HOUR = "hour";
	public static final String MONGO_SHOP_REALTIME_HOUR_ISCUSTOMER= "isCustomer";
	public static final String MONGO_SHOP_REALTIMEHOUR_MACS = "macs";

	public static final String MONGO_COLLECTION_SHOP_STATICINFO = "shop_static_info";
	public static final String MONGO_STATICINFO_SHOP_SCENEID = "sceneId";
	public static final String MONGO_STATICINFO_SHOP_OPENMINUTE = "openMinute";
	public static final String MONGO_STATICINFO_SHOP_CLOSEMINUTE = "closeMinute";

}
