package jmx_logger_qa_screen;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cassandra.DbFunctions;
import cassandra.RunExternalCassandraStress;
import general.Chart;
import general.LoadPropertiesFile;
import general.Miscellaneous;
import jmx.JmxListener;

public class TestScreen {
	
	private static final Logger logger = LogManager.getLogger(Class.class.getName());
	
	public static void main(String[] args)
			throws InterruptedException, ExecutionException, IOException, MalformedObjectNameException,
			AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException {
		logger.info("================================================================================");
		logger.info("  Starting TestScreen.main...");
		
		Integer querySleepInterval = Integer.parseInt(LoadPropertiesFile.getProperty("querySleepInterval"));
		String testResultsKeyspace = LoadPropertiesFile.getProperty("testResultsKeyspace");
		String testResultsTable = LoadPropertiesFile.getProperty("testResultsTable");
		TreeMap<String, String> readMetricHash = new TreeMap<>();
		TreeMap<String, String> validatedMetricHash = new TreeMap<>();
		
		// Create a thread for the cassandra-stress process.
		long startTime = System.currentTimeMillis();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<String> future = executorService.submit(() -> {
			RunExternalCassandraStress.runExternalCassandraStress();
			return "Cassadra-stress has completed.";
		});

		// Connect to Cassandra; ensure it is running.
		DbFunctions.createSession();

		// Connect to the JMX listener.
		JmxListener.createConnectionToJmxService();
		
		// While the cassandra-stress is running, poll with the jmx listener and gather
		// metrics.
		// Sleep at a set interval before retrieving metrics again.
		while (!future.isDone()) {
			// Retrieve the metrics from JMX. Use the default cassandra-space keyspace 'keyspace1'.
			readMetricHash = (TreeMap<String, String>) JmxListener.getJmxMetrics(readMetricHash, "keyspace1");
			
			// Every 30 seconds mention that we're waiting on cassandra-stress.
			Miscellaneous.checkTime(startTime, "cassandra-stress");
			
			Thread.sleep(querySleepInterval);
		}
		
		String result = future.get();
		logger.info(result);
		RunExternalCassandraStress.close();

		// Validate the JMX results.
		validatedMetricHash = (TreeMap<String, String>) Miscellaneous.validateMetricHash(readMetricHash);
		
		// Create the chart
		Chart.createChartHTML(validatedMetricHash);
		
		// Write the results as read from JMX to the Cassandra database.
		DbFunctions.writeResults(testResultsKeyspace, testResultsTable, readMetricHash);

		// Close the connection to the JMX Listener.
		JmxListener.close();
		
		// Close the connection to the database.
		DbFunctions.close();
		
		// Shutdown the cassandra-stress process thread.
		executorService.shutdown();
		
		logger.info("Done...");
	}
	

}