package jmx;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedMap;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import general.LoadPropertiesFile;

public class JmxListener {

	private JmxListener() {
	}

	private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static JMXConnector jmxConnector;
	private static MBeanServerConnection jmxConnection;

	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
	private static final SimpleDateFormat FORMATTED_DATE = new SimpleDateFormat(DATE_PATTERN);

	private static final String NODE_IP = LoadPropertiesFile.getProperty("nodeIp");
	private static final String JMX_PORT = LoadPropertiesFile.getProperty("jmxPort");

	private static final Integer LIVESSTABLECOUNT_THRESHOLD = Integer.parseInt(LoadPropertiesFile.getProperty("liveSSTableCountThreshold"));
	private static final Long ALLMEMTABLESLIVEDATASIZE_THRESHOLD = Long.parseLong(LoadPropertiesFile.getProperty("allMemtablesLiveDataSizeThreshold"));
	private static final Double READLATENCY_THRESHOLD = Double.parseDouble(LoadPropertiesFile.getProperty("readLatencyThreshold"));
	private static final Double WRITELATENCY_THRESHOLD = Double.parseDouble(LoadPropertiesFile.getProperty("writeLatencyThreshold"));


	public static MBeanServerConnection createConnectionToJmxService() throws IOException {
		logger.info("================================================================================");
		logger.info("  Starting JmxListener.createConnectionToJmxService...");

		JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%1$s:%2$s/jndi/rmi://%1$s:%2$s/jmxrmi", NODE_IP, JMX_PORT));

	    jmxConnector = JMXConnectorFactory.connect(url, null);
	    jmxConnection = jmxConnector.getMBeanServerConnection();

		return jmxConnection;
	}

	public static void close() throws IOException {
		jmxConnector.close();
	}

	public static SortedMap<String, String> getJmxMetrics(SortedMap<String, String> readMetricHash, String keyspace) throws MalformedObjectNameException, AttributeNotFoundException,
			InstanceNotFoundException, MBeanException, ReflectionException, IOException {
		logger.debug("================================================================================");
		logger.debug("  Starting JmxListener.getJmxMetrics from keyspace '" + keyspace + "'...");

		// The cassandra-stress is being run in mixed mode, so we'll tablulate for both standard1 and counter1.
		ObjectName liveSSTableCountPattern1 = new ObjectName("org.apache.cassandra.metrics:keyspace=" + keyspace + ",name=LiveSSTableCount,scope=standard1,type=ColumnFamily");
		ObjectName liveSSTableCountPattern2 = new ObjectName("org.apache.cassandra.metrics:keyspace=" + keyspace + ",name=LiveSSTableCount,scope=counter1,type=ColumnFamily");
		ObjectName allMemtablesLiveDataSizePattern1 = new ObjectName("org.apache.cassandra.metrics:keyspace=" + keyspace + ",name=AllMemtablesLiveDataSize,scope=standard1,type=ColumnFamily");
		ObjectName allMemtablesLiveDataSizePattern2 = new ObjectName("org.apache.cassandra.metrics:keyspace=" + keyspace + ",name=AllMemtablesLiveDataSize,scope=counter1,type=ColumnFamily");

		ObjectName readLatencyPattern = new ObjectName("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency");
		ObjectName writeLatencyPattern = new ObjectName("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency");

		Integer liveSSTableCountTotal = ((Integer) jmxConnection.getAttribute(liveSSTableCountPattern1, "Value") + (Integer) jmxConnection.getAttribute(liveSSTableCountPattern2, "Value"));
		Long allMemtablesLiveDataSizeTotal = ((Long) jmxConnection.getAttribute(allMemtablesLiveDataSizePattern1, "Value") + (Long) jmxConnection.getAttribute(allMemtablesLiveDataSizePattern2, "Value"));
		Double readLatency95thPercentileTotal = (Double) jmxConnection.getAttribute(readLatencyPattern, "95thPercentile");
		Double writeLatency95thPercentileTotal = (Double) jmxConnection.getAttribute(writeLatencyPattern, "95thPercentile");

		checkThresholds(liveSSTableCountTotal, allMemtablesLiveDataSizeTotal, readLatency95thPercentileTotal, writeLatency95thPercentileTotal);

		// Now that we've performed our calculations, convert back to String so we can send the values back in the hash.
		String liveSSTableCount = Long.toString(liveSSTableCountTotal);
		String allMemtablesLiveDataSize = Long.toString(allMemtablesLiveDataSizeTotal);
		String readLatency95thPercentile = Double.toString(readLatency95thPercentileTotal);
		String writeLatency95thPercentile = Double.toString(writeLatency95thPercentileTotal);

		String timestamp = FORMATTED_DATE.format(new Date());

		logger.debug("timestamp                 : " + timestamp);
		logger.debug("liveSSTableCount          : " + liveSSTableCount);
		logger.debug("allMemtablesLiveDataSize  : " + allMemtablesLiveDataSize);
		logger.debug("readLatency95thPercentile: " + readLatency95thPercentile);
		logger.debug("writeLatency95thPercentile: " + writeLatency95thPercentile);

		readMetricHash.put(timestamp, String.join(",", liveSSTableCount, allMemtablesLiveDataSize, readLatency95thPercentile, writeLatency95thPercentile));

		return readMetricHash;
	}

	public static void checkThresholds(Integer liveSSTableCountTotal, Long allMemtablesLiveDataSizeTotal, Double readLatency95thPercentileTotal, Double writeLatency95thPercentileTotal) {
		logger.debug("================================================================================");
		logger.debug("  Starting JmxListener.checkThresholds ...");

		if (liveSSTableCountTotal > LIVESSTABLECOUNT_THRESHOLD) {
			logger.warn("Read LiveSSTableCount '" + liveSSTableCountTotal + "' is greater than the threshold '" + LIVESSTABLECOUNT_THRESHOLD +  "'!");
		}

		if (allMemtablesLiveDataSizeTotal > ALLMEMTABLESLIVEDATASIZE_THRESHOLD) {
			logger.warn("Read AllMemtablesLiveDataSize '" + allMemtablesLiveDataSizeTotal + "' is greater than the threshold '" + ALLMEMTABLESLIVEDATASIZE_THRESHOLD +  "'!");
		}

		if (readLatency95thPercentileTotal > READLATENCY_THRESHOLD) {
			logger.warn("Read ReadLatency95thPercentile '" + readLatency95thPercentileTotal + "' is greater than the threshold '" + READLATENCY_THRESHOLD +  "'!");
		}

		if (writeLatency95thPercentileTotal > WRITELATENCY_THRESHOLD) {
			logger.warn("Read WriteLatency95thPercentile '" + writeLatency95thPercentileTotal + "' is greater than the threshold '" + WRITELATENCY_THRESHOLD +  "'!");
		}
	}

}
