/**
 * @author caoyupeng@doodod.com
 */
package com.palmap.rssi.common;

public class Common {
	public static final String BRAND_UNKNOWN      = "unknown";

	public static final int MAC_KEY_LENGTH = 8;
	public static final String CTRL_A = "\u0001";
	public static final String CTRL_B = "\u0002";
	public static final String CTRL_C = "\u0003";
	public static final String CTRL_D = "\u0004";
	public static final String TAB = "\t";
	public static final char BATCH_INTERVAL_IN_MILLI_SEC = 60000;
	public static final int MAX_TIMES = 20;
	public static final int DAY_FORMATER = 60000 * 60 * 24;
	public static final long DEFAULT_DWELL_PASSENGER = 60000 * 15;
	public static final int MINUTE_FORMATER = 60000;
	public static final int DEFAULT_UNIT_DWELL = BATCH_INTERVAL_IN_MILLI_SEC / MINUTE_FORMATER;
	public static final String NOW_MINUTE_FORMAT = "yyyy-MM-dd HH:mm:00";
	public static final String NOW_HOUR_FORMAT = "yyyy-MM-dd HH:00:00";
	public static final String TODAY_FIRST_TS_FORMAT = "yyyy-MM-dd 00:00:00";


	public static final int CUSTOMER_VALUE = 0;
	public static final int PASSENGER_VALUE = 1;
	public static final int EMPLOYEE_VALUE = 2;
	public static final int MACHINE_VALUE = 1;




	public static final String SPARK_CONFIG = "sparkRssiInfo.xml";
	public static final String MONGO_DB_NAME = "mongo.db.name";
	public static final String MONGO_ADDRESS_LIST = "mongo.address.list";
	public static final String MONGO_SERVER_PORT= "mongo.server.port";


	public static final String MONGO_OPTION_PUSH = "$push";
	public static final String MONGO_OPTION_INC = "$inc";
	public static final String MONGO_OPTION_SET  = "$set";
	public static final String MONGO_OPTION_UNSET  = "$unset";
	public static final String MONGO_OPTION_SLICE  = "$slice";
	public static final String MONGO_OPTION_EACH  = "$each";
	public static final String MONGO_OPTION_ALL  = "$all";
	public static final String MONGO_OPTION_SIZE  = "$size";
	public static final String MONGO_OPTION_ADDTOSET  = "$addToSet";


	public static final String MONGO_COLLECTION_VISITED = "shop_visited";
	public static final String MONGO_VISITED_DATE = "date";
	public static final String MONGO_VISITED_LOCATIONID = "locationId";
	public static final String MONGO_VISITED_SCENEID = "sceneId";
	public static final String MONGO_VISITED_MAC = "mac";
	//public static final String MONGO_VISITED_FREQUENCY = "frequency";
	public static final String MONGO_VISITED_DWELL = "dwell";
	public static final String MONGO_VISITED_TIMES= "times";
	public static final String MONGO_VISITED_USERTYPE= "userType";
	public static final String MONGO_VISITED_PHONEBRAND= "brand";

	public static final String MONGO_COLLECTION_HISTORY = "shop_history";
	public static final String MONGO_HISTORY_LOCATIONID = "locationId";
	public static final String MONGO_HISTORY_SCENEID = "sceneId";
	public static final String MONGO_HISTORY_MAC = "mac";
	public static final String MONGO_HISTORY_TIMES = "times";
	public static final String MONGO_HISTORY_DAYS = "days";

	public static final String MONGO_COLLECTION_REALTIME = "shop_realtime";
	public static final String MONGO_REALTIME_LOCATIONID = "locationId";
	public static final String MONGO_REALTIME_SCENEID = "sceneId";
	public static final String MONGO_REALTIME_TIME = "time";
	public static final String MONGO_REALTIME_USERTYPE= "userType";
	public static final String MONGO_REALTIME_MACS = "macs";
	public static final String MONGO_REALTIME_MACSUM = "macSum";

	public static final String MONGO_COLLECTION_REALTIME_HOUR= "shop_realtime_hour";
	public static final String MONGO_REALTIME_HOUR_LOCATIONID  = "locationId";
	public static final String MONGO_REALTIME_HOUR_SCENEID = "sceneId";
	public static final String MONGO_REALTIME_HOUR = "hour";
	public static final String MONGO_REALTIME_HOUR_USERTYPE= "userType";
	public static final String MONGO_REALTIMEHOUR_MACS = "macs";

	public static final String MONGO_STORE_APMAC_RELATION= "store_apmac_relation";
	public static final String MONGO_USER_TYPE_INFO= "user_type_info";

}
