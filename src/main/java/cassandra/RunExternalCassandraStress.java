package cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import general.LoadPropertiesFile;

public class RunExternalCassandraStress {

	private RunExternalCassandraStress() {
	}

	private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static final String DATE_PATTERN = "yyyy-MM-dd_HH:mm:ss";
	private static final SimpleDateFormat FORMATTED_DATE = new SimpleDateFormat(DATE_PATTERN);
	private static final String CURRENT_DATE = FORMATTED_DATE.format(new Date());

	private static final Integer JMX_PORT = Integer.parseInt(LoadPropertiesFile.getProperty("jmxPort"));
	private static final Integer NODE_PORT = Integer.parseInt(LoadPropertiesFile.getProperty("nodePort"));
	private static final String NODE_IP = LoadPropertiesFile.getProperty("nodeIp");
	private static final String NODE_USERNAME = LoadPropertiesFile.getProperty("nodeUsername");
	private static final String NODE_PASSWORD = LoadPropertiesFile.getProperty("nodePassword");
	private static final String LOG_DIR = LoadPropertiesFile.getProperty("logDir");
	private static final String GRAPH_DIR = LoadPropertiesFile.getProperty("graphDir");

	private static final Integer NUMBER_OF_WRITES = Integer.parseInt(LoadPropertiesFile.getProperty("numberOfWrites"));

	private static Process process;


	/**
	 * Run the Cassandra stress logic.
	 *
	 * @throws IOException
	 */
	public static void runExternalCassandraStress() throws IOException {
		logger.info("================================================================================");
		logger.info("  Starting RunExternalCassandraStress.runExternalCassandraStress...");

		String auth = "";
		if ((NODE_USERNAME != null && !NODE_USERNAME.isEmpty()) &&
				(NODE_PASSWORD != null && !NODE_PASSWORD.isEmpty())) {
			auth = "user=" + NODE_USERNAME + " password=" + NODE_PASSWORD;
		}

		// The jmx_logger_qa_screen.md instructions call for "Download and install
		// Cassandra on a Linux Based System".
		// We'll therefore use the executable location of /usr/bin/ and not rely upon
		// the executable being in the $PATH, %PATH%, etc.
		// Note: no custom yaml config file is being referenced in this process, therefore the default keyspace 'keyspace1' will be used.

		String cassandraStressExecute = " mixed n=" + NUMBER_OF_WRITES + " cl=one" +
		" -mode native cql3 " + auth +
		" -node " + NODE_IP + " -port native=" + NODE_PORT + " jmx=" + JMX_PORT +
		" -rate threads>=25 threads<=100 auto" +
		" -graph file=" + GRAPH_DIR + "cassandra-stress_" + CURRENT_DATE + ".html title=test_" + CURRENT_DATE + " revision=" + CURRENT_DATE +
		" -log file=" + LOG_DIR + "cassandra-stress_" + CURRENT_DATE + ".log";

		logger.debug("cassandraStressExecute: ");
		logger.debug(cassandraStressExecute);

		process = new ProcessBuilder("/usr/bin/cassandra-stress", cassandraStressExecute).start();

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;

		logger.debug("Output of cassandra-stress is:");

		while ((line = br.readLine()) != null) {
			logger.debug(line);
		}
	}

	public static void close() {
		process.destroy();
	}
}
