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

	public static final String MONGO_COLLECTION_SHOP_VISITED = "Shop_Visited";
	public static final String MONGO_SHOP_VISITED_DATE = "date";
//	public static final String MONGO_SHOP_VISITED_LOCATIONID = "locationId";
	public static final String MONGO_SHOP_VISITED_SCENEID = "sceneId";
	public static final String MONGO_SHOP_VISITED_MAC = "mac";
	public static final String MONGO_SHOP_VISITED_DWELL = "dwell";
	public static final String MONGO_SHOP_VISITED_TIMES = "times";
	public static final String MONGO_SHOP_VISITED_USERTYPE = "userType";
	public static final String MONGO_SHOP_VISITED_PHONEBRAND = "brand";
	public static final String MONGO_SHOP_VISITED_ISCUSTOMER = "isCustomer";

	public static final String MONGO_COLLECTION_HISTORY = "Shop_History";
	public static final String MONGO_HISTORY_LOCATIONID = "locationId";
	public static final String MONGO_HISTORY_SCENEID = "sceneId";
	public static final String MONGO_HISTORY_MAC = "mac";
	public static final String MONGO_HISTORY_TIMES = "times";
	public static final String MONGO_HISTORY_FIRSTDATE = "firstDate";
	public static final String MONGO_HISTORY_LASTDATE = "lastDate";
	public static final String MONGO_HISTORY_DAYS = "days";

	public static final String MONGO_COLLECTION_REALTIME = "Shop_Realtime";
	public static final String MONGO_REALTIME_LOCATIONID = "locationId";
	public static final String MONGO_REALTIME_SCENEID = "sceneId";
	public static final String MONGO_REALTIME_TIME = "time";
//	public static final String MONGO_REALTIME_USERTYPE= "userType";
	public static final String MONGO_REALTIME_MACS = "macs";
	public static final String MONGO_REALTIME_MACSUM = "macSum";

	public static final String MONGO_COLLECTION_REALTIME_HOUR= "Shop_Realtime_Hour";
//	public static final String MONGO_REALTIME_HOUR_LOCATIONID  = "locationId";
	public static final String MONGO_REALTIME_HOUR_SCENEID = "sceneId";
	public static final String MONGO_REALTIME_HOUR = "hour";
//	public static final String MONGO_REALTIME_HOUR_USERTYPE = "userType";
	public static final String MONGO_REALTIMEHOUR_MACS = "macs";

	public static final String MONGO_COLLECTION_STORE_APMAC = "Shop_Store_ApMac";
	public static final String MONGO_STORE_APMAC_APSTATUS   = "apStatus";
	public static final String MONGO_STORE_APMAC_APMAC      = "apMac";
	public static final String MONGO_STORE_APMAC_DATE       = "date";
	public static final String MONGO_STORE_APMAC_SCENEID    = "sceneId";
	public static final String MONGO_STORE_APMAC_STORENAME  = "storeName";

	public static final String MONGO_COLLECTION_STATICINFO = "Static_Info";
	public static final String MONGO_STATICINFO_SCENEID = "sceneId";
	public static final String MONGO_STATICINFO_OPENMINUTE = "openMinute";
	public static final String MONGO_STATICINFO_CLOSEMINUTE = "closeMinute";

}
