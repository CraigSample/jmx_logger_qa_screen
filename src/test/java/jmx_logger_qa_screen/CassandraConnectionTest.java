package jmx_logger_qa_screen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import general.LoadPropertiesFile;

public class CassandraConnectionTest {

	private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static Cluster cluster;

	private static Session session;

	public static void main(String[] args) {

		logger.info("================================================================================");
		logger.info("  Starting CassandraConnectionTest.main...");

		Integer nodePort = Integer.parseInt(LoadPropertiesFile.getProperty("nodePort"));
		String nodeIp = LoadPropertiesFile.getProperty("nodeIp");
		String nodeUsername = LoadPropertiesFile.getProperty("nodeUsername");
		String nodePassword = LoadPropertiesFile.getProperty("nodePassword");

		try {

			Builder b = Cluster.builder().addContactPoint(nodeIp).withCredentials(nodeUsername, nodePassword);
			if (nodePort != null) {
				b.withPort(nodePort);
			}
			cluster = b.build();

			session = cluster.connect();

			ResultSet rs = session.execute("select release_version from system.local");
			Row row = rs.one();

			String releaseVersion = row.getString("release_version");

			if ((releaseVersion != null) && (!releaseVersion.isEmpty())) {
				logger.info("Cassandra is running with release version " + releaseVersion);
			} else {
				logger.error("Cassandra connection test failed: no release version found!");
			}

			close();
		} catch (Exception e) {
			logger.error("Cassandra connection test failed: " + e);
		}

		logger.info("Done...");
	}

	public static void close() {
		session.close();
		cluster.close();
	}

}
