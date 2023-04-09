package cassandra;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.Cluster.Builder;

import general.LoadPropertiesFile;
import general.Miscellaneous;

public class DbFunctions {

	private DbFunctions() {
	}

	private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static final Integer NODE_PORT = Integer.parseInt(LoadPropertiesFile.getProperty("nodePort"));
	private static final String NODE_IP = LoadPropertiesFile.getProperty("nodeIp");
	private static final String NODE_USERNAME = LoadPropertiesFile.getProperty("nodeUsername");
	private static final String NODE_PASSWORD = LoadPropertiesFile.getProperty("nodePassword");

	private static final Integer MAXIMUM_BATCH_ENTRIES = Integer.parseInt(LoadPropertiesFile.getProperty("maximumBatchEntries"));

	private static Cluster cluster;
	private static Session session;


	/**
	 * Create a session for connecteing to the database.
	 *
	 * @return Session
	 */
	public static Session createSession() {

		logger.info("================================================================================");
		logger.info("  Starting DbFunctions.createSession...");

		try {

			Builder b = Cluster.builder().addContactPoint(NODE_IP);

			if ((NODE_USERNAME != null && !NODE_USERNAME.isEmpty()) &&
					(NODE_PASSWORD != null && !NODE_PASSWORD.isEmpty())) {
				b.withCredentials(NODE_USERNAME, NODE_PASSWORD);
			}

			if (NODE_PORT != null) {
				b.withPort(NODE_PORT);
			}

			try {
				cluster = b.build();

				session = cluster.connect();
			} catch (AuthenticationException e) {
				logger.error("Cassandra authentication error! Check your user name and password for '" + NODE_USERNAME + "'.");
				System.exit(1);
			}

			ResultSet rs = session.execute("select release_version from system.local");
			Row row = rs.one();

			String releaseVersion = row.getString("release_version");

			if ((releaseVersion != null) && (!releaseVersion.isEmpty())) {
				logger.info("Cassandra is running with release version " + releaseVersion);
			} else {
				logger.error("Cassandra DbFuntions test failed: no release version found!");
			}

		} catch (Exception e) {
			logger.error("Cassandra DbFuntions test failed: " + e);
		}

		return session;
	}

	public static void close() {
		session.close();
		cluster.close();
	}


	/**
	 * Get the database session.
	 *
	 * @return Session
	 */
	public static Session getSession() {
		return session;
	}


	/**
	 * Get the database cluster.
	 *
	 * @return Cluster
	 */
	public static Cluster getCluster() {
		return cluster;
	}


	/**
	 * Create a keyspace with the given name.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 */
	public static void createKeyspace(String keyspaceName) {
		logger.info("================================================================================");
		logger.info("  Starting DbFunctions.createKeyspace '" + keyspaceName + "'...");

		// Using NetworkTopologyStrategy and datacenter1 as these are the current
		// environment:
		//		craig@zoidberg:~/eclipse-workspace/jmx_logger_qa_screen/$ nodetool status
		//		Datacenter: datacenter1
		//		=======================
		//		Status=Up/Down
		//		|/ State=Normal/Leaving/Joining/Moving
		//		--  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
		//		UN  127.0.0.1  24.7 MiB   256          100.0%            84c88e5e-9bf0-4e9e-8033-66b45b4d48df  rack1

		StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName)
				.append(" WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };");

		String query = sb.toString();
		session.execute(query);
	}


	/**
	 * Create a test table with the given name in the given keyspace.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 * @param tableName The name of the table to create.
	 */
	public static void createTestTable(String keyspaceName, String tableName) {
		logger.info("================================================================================");
		logger.info("  Starting DbFunctions.createTestTable '" + keyspaceName + "." + tableName + "'...");

		String table = keyspaceName + "." + tableName;

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (")
				.append(" timestamp timestamp PRIMARY KEY,")
				.append(" liveSSTableCount int,")
				.append(" allMemtablesLiveDataSize int,")
				.append(" readLatency95thPercentile double,")
				.append(" writeLatency95thPercentile double);");

		String query = sb.toString();
		session.execute(query);
	}


	/**
	 * Write the given map results to the given test table in the given keyspace.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 * @param tableName The name of the table to create.
	 * @param passedMap The map of data to write.
	 */
	public static void writeMapResultsToTable(String keyspaceName, String tableName, SortedMap<String, String> passedMap) {
		logger.info("================================================================================");
		logger.info("  Starting DbFunctions.writeMapResultsToTable '" + keyspaceName + "." + tableName + "'...");

		String table = keyspaceName + "." + tableName;
		String lineSeparator = System.getProperty("line.separator");

		// The APPLY BATCH method will not work if we exceed 2147483647 characters, or batch_size_warn_threshold_in_kb apparently. ;)
		// Break the inserts into 400 line batches or so to start.
		int originalMapSize = passedMap.size();
		int numberOfLoops = (originalMapSize + MAXIMUM_BATCH_ENTRIES - 1) / MAXIMUM_BATCH_ENTRIES;

		int entryCounterStart = 0;
		int entryCounterEnd = MAXIMUM_BATCH_ENTRIES;
		for (int loop = 1; loop <= numberOfLoops; loop++) {

			TreeMap<String, String> subMap = (TreeMap<String, String>) Miscellaneous.getRangeOfSortedMapEntries(entryCounterStart, entryCounterEnd, passedMap);

			StringBuilder sb = new StringBuilder("BEGIN UNLOGGED BATCH" + lineSeparator);

			for (Map.Entry<String, String> entry : subMap.entrySet()) {

                String metricKey = entry.getKey();
                String metricValues = entry.getValue();
                String insertValues = "('" + metricKey + "', " + metricValues  + ");" + lineSeparator;

                String insertString = " INSERT INTO " + table +
                                " (timestamp, liveSSTableCount, allMemtablesLiveDataSize, readLatency95thPercentile, writeLatency95thPercentile)" +
                                "  VALUES " + insertValues;
                sb.append(insertString);

			}

            sb.append("APPLY BATCH;");

            String queryString = sb.toString();

            logger.error(queryString);
            logger.error("Size of query: " + queryString.length());

            String query = sb.toString();

            try {
            	session.execute(query);
            } catch (InvalidQueryException e1) {
            	logger.error("Batch is too large, you need to lower the maximum entries from '" + MAXIMUM_BATCH_ENTRIES + "'.");
            	logger.error("entryCounterStart: " + entryCounterStart);
            	logger.error("entryCounterEnd: " + entryCounterEnd);
            }

			entryCounterStart+=MAXIMUM_BATCH_ENTRIES;
			entryCounterEnd+=MAXIMUM_BATCH_ENTRIES;
		}

	}


	/**
	 * Write the given map results to the given test table in the given keyspace.
	 * Create the given  keyspace or table if they don't already exist.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 * @param tableName The name of the table to create.
	 * @param passedMap The map of data to write.
	 */
	public static void writeResults(String keyspaceName, String tableName, SortedMap<String, String> passedMap) {
		logger.info("================================================================================");
		logger.info("  Starting DbFunctions.writeResults '" + keyspaceName + "." + tableName + "'...");

		createKeyspace(keyspaceName);

		createTestTable(keyspaceName, tableName);

		writeMapResultsToTable(keyspaceName, tableName, passedMap);
	}


	/**
	 * Create the standard stress test database table in the given keyspace and table.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 * @param tableName The name of the table to create.
	 */
	public static void createStressTableStandard(String keyspaceName, String tableName) {
		logger.debug("================================================================================");
		logger.debug("  Starting DbFunctions.createStressTableStandard for '" + keyspaceName + "." + tableName + "'...");

		String table = keyspaceName + "." + tableName;

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (")
				.append(" key blob PRIMARY KEY,")
				.append(" \"C0\" blob,")
				.append(" \"C1\" blob,")
				.append(" \"C2\" blob,")
				.append(" \"C3\" blob,")
				.append(" \"C4\" blob)")
				.append(" WITH COMPACT STORAGE")
				.append(" AND bloom_filter_fp_chance = 0.01")
				.append(" AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}")
				.append(" AND comment = ''")
				.append(" AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}")
				.append(" AND compression = {'enabled': 'false'}")
				.append(" AND crc_check_chance = 1.0")
				.append(" AND dclocal_read_repair_chance = 0.1")
				.append(" AND default_time_to_live = 0")
				.append(" AND gc_grace_seconds = 864000")
				.append(" AND max_index_interval = 2048")
				.append(" AND memtable_flush_period_in_ms = 0")
				.append(" AND min_index_interval = 128")
				.append(" AND read_repair_chance = 0.0")
				.append(" AND speculative_retry = '99PERCENTILE';");

		String query = sb.toString();
		session.execute(query);
	}



	/**
	 * Create the standard stress test database table counter in the given keyspace and table.
	 *
	 * @param keyspaceName The name of the keyspace to create.
	 * @param tableName The name of the table to create.
	 */
	public static void createStressTableCounter(String keyspaceName, String tableName) {
		logger.debug("================================================================================");
		logger.debug("  Starting DbFunctions.createStressTableCounter for '" + keyspaceName + "." + tableName + "'...");

		String table = keyspaceName + "." + tableName;

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (")
				.append("key blob,")
					    .append("column1 text,")
					    .append("\"C0\" counter static,")
					    .append("\"C1\" counter static,")
					    .append("\"C2\" counter static,")
					    .append("\"C3\" counter static,")
					    .append("\"C4\" counter static,")
					    .append("value counter,")
					    .append("PRIMARY KEY (key, column1))")
					    .append(" WITH COMPACT STORAGE")
					    .append(" AND CLUSTERING ORDER BY (column1 ASC)")
					    .append(" AND bloom_filter_fp_chance = 0.01")
					    .append(" AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}")
					    .append(" AND comment = ''")
					    .append(" AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}")
					    .append(" AND compression = {'enabled': 'false'}")
					    .append(" AND crc_check_chance = 1.0")
			    		.append(" AND dclocal_read_repair_chance = 0.1")
			    		.append(" AND default_time_to_live = 0")
			    		.append(" AND gc_grace_seconds = 864000")
			    		.append(" AND max_index_interval = 2048")
			    		.append(" AND memtable_flush_period_in_ms = 0")
			    		.append(" AND min_index_interval = 128")
			    		.append(" AND read_repair_chance = 0.0")
			    		.append(" AND speculative_retry = '99PERCENTILE';");

		String query = sb.toString();
		session.execute(query);
	}

}
