package jmx;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import general.LoadPropertiesFile;
import general.Miscellaneous;

public class RunExternalJmxTerm {

    private RunExternalJmxTerm() {
    }

    private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static Process process;

	public static SortedMap<String, String> runExternalJmxTerm(String keyspace) throws IOException {
		logger.info("================================================================================");
		logger.info("  Starting RunExternalJmxTerm.runExternalJmxTerm...");

		Integer jmxPort = Integer.parseInt(LoadPropertiesFile.getProperty("jmxPort"));
		String nodeIp = LoadPropertiesFile.getProperty("nodeIp");

		String shellScript = "runJmxterm.sh";
		String shellScriptText = "#!/usr/bin/expect -f\n" +
				"		set CHILD_PID [spawn java -jar libs/jmxterm-1.0.0-uber.jar -n -v silent -l service:jmx:rmi:///jndi/rmi://" + nodeIp + ":" +  jmxPort + "/jmxrmi]\n" +
				"		send \"domain org.apache.cassandra.metrics\\r\"\n" +
				"		while { true } {\n" +
				"		        set now [clock seconds]\n" +
				"		        set date [clock format $now -format {%D %T}]\n" +
				"		        set date\n" +
				"		        puts $date\n" +
				"		        send \"get -s -b keyspace=" + keyspace + ",name=LiveSSTableCount,scope=standard1,type=ColumnFamily Value && " +
				"get -s -b keyspace=" + keyspace + ",name=LiveSSTableCount,scope=counter1,type=ColumnFamily Value && " +
				"get -s -b keyspace=" + keyspace + ",name=AllMemtablesLiveDataSize,scope=standard1,type=ColumnFamily Value && " +
				"get -s -b keyspace=" + keyspace + ",name=AllMemtablesLiveDataSize,scope=counter1,type=ColumnFamily Value && " +
				"get -s -b type=ClientRequest,scope=Read,name=Latency 95thPercentile && " +
				"get -s -b type=ClientRequest,scope=Write,name=Latency 95thPercentile\\r\"\n" +
				"		        expect sleep 1\n" +
				"		}\n" +
				"\n" +
				"send \"quit \\n\"\n" +
				"expect eof";

		File file = new File(shellScript);
		Files.deleteIfExists(file.toPath());

		PrintWriter printWriter = new PrintWriter(file);

		printWriter.println(shellScriptText);
		printWriter.close();

		process = new ProcessBuilder("/usr/bin/expect", shellScript).start();

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;

		logger.debug("Output of running jmxterm is:");

		TreeMap<String, String> readMetricHash = new TreeMap<>();
		String[] readMetricArray = new String[7];

		int cnt = 0;
		while ((line = br.readLine()) != null) {
			if ((!line.startsWith("domain")) && (!line.startsWith("get")) && (!line.startsWith("spawn"))) {
				logger.debug(cnt + " " + line);

				readMetricArray[cnt] = line;

				if (cnt == 6) {

					String timestamp = readMetricArray[0];

					// The cassandra-stress is being run in mixed mode, so we'll tablulate for both standard1 and counter1.
					Integer liveSSTableCountTotal = (int) (Miscellaneous.convertStringValue(readMetricArray[1], "LiveSSTableCount standard1", timestamp) +
							Miscellaneous.convertStringValue(readMetricArray[2], "LiveSSTableCount counter1", timestamp));

					Long allMemtablesLiveDataSizeTotal = (long) (Miscellaneous.convertStringValue(readMetricArray[3], "AllMemtablesLiveDataSize standard1", timestamp) +
							Miscellaneous.convertStringValue(readMetricArray[4], "AllMemtablesLiveDataSize counter1", timestamp));

					Double readLatency95thPercentileTotal = Miscellaneous.convertStringValue(readMetricArray[5], "ReadLatency95thPercentileTotal", timestamp);
					Double writeLatency95thPercentileTotal = Miscellaneous.convertStringValue(readMetricArray[6], "WriteLatency95thPercentileTotal", timestamp);

					JmxListener.checkThresholds(liveSSTableCountTotal, allMemtablesLiveDataSizeTotal, readLatency95thPercentileTotal, writeLatency95thPercentileTotal);

					readMetricHash.put(timestamp,
							String.join(",",
							Objects.toString(liveSSTableCountTotal, null),
							Objects.toString(allMemtablesLiveDataSizeTotal, null),
							Objects.toString(readLatency95thPercentileTotal, null),
							Objects.toString(writeLatency95thPercentileTotal, null)));
					cnt = 0;
					readMetricArray = new String[7]; // ensure that we 'zero' out the values for the next pass.
				} else {
					cnt++;
				}
			}
		}

		return readMetricHash;
	}

	public static void close() {
		process.destroy();
	}

}
