package general;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WritePropertiesFile {
	
	private static final Logger logger = LogManager.getLogger(Class.class.getName());
	
	/**
	 * @param args Main passed arguments.
	 */
	public static void main(String[] args) {
		
		logger.info("================================================================================");
		logger.info("  Starting WritePropertiesFile...");

		Properties prop = new Properties();
		OutputStream output = null;

		try {
			output = new FileOutputStream("config.properties");
			
			// Cassandra DB
			prop.setProperty("nodeIp", "127.0.0.1");
			prop.setProperty("nodePort", "9042");
			prop.setProperty("nodeUsername", "craig");
			prop.setProperty("nodePassword", "badpassword1");
			prop.setProperty("jmxPort", "7199");
			
			// We shouldn't have the JMX listener poll the Cassandra db constantly, throttle the requests by setting this interval value.
			prop.setProperty("querySleepInterval", "1000"); // in milliseconds
			
			// While recording JMX values from a running Cassandra Instance check that certain thresholds are not exceeded.
			// The following sets the thresholds to check, and are arbitrary to the system being monitored. Your mileage may vary.
			prop.setProperty("liveSSTableCountThreshold", "11");
			prop.setProperty("allMemtablesLiveDataSizeThreshold", "38500000");
			prop.setProperty("readLatencyThreshold", "800000");
			prop.setProperty("writeLatencyThreshold", "400");

			// Output test results table and keyspace
			prop.setProperty("testResultsKeyspace", "JmxMetrics");
			prop.setProperty("testResultsTable", "TestResults");

			// The APPLY BATCH method will not work if we exceed 120000 characters or so, or batch_size_warn_threshold_in_kb apparently. ;)
			// Break the inserts into 500 line batches to start. Should it error out, you can use the entries in the error.log to manually insert.
			prop.setProperty("maximumBatchEntries", "100");
			
			// Output cassandra-stress tables and keyspace
//			prop.setProperty("cassandraStressKeyspace", "keyspace1");
//			prop.setProperty("cassandraStressTableStandard", "standard1");
//			prop.setProperty("cassandraStressTableCounter", "counter1");
			
			// Number of writes for cassandra-stress 
			prop.setProperty("numberOfWrites", "1000000");
			
			// Output log and graph locations
			String slash = System.getProperty("file.separator");
			prop.setProperty("logDir", "logs" + slash);
			prop.setProperty("graphDir", "graphs" + slash);

			// save properties to project root folder
			prop.store(output, null);

			logger.info("Done...");

		} catch (IOException io) {
			logger.catching(io);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					logger.catching(e);
				}
			}
		}
	}
}
