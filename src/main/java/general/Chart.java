package general;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Chart {

	private static final Logger logger = LogManager.getLogger(Class.class.getName());

	private static final String DATE_PATTERN = "yyyy-MM-dd_HH:mm:ss";
	private static final SimpleDateFormat FORMATTED_DATE = new SimpleDateFormat(DATE_PATTERN);
	private static final String CURRENT_DATE = FORMATTED_DATE.format(new Date());

	private static final String LIVESSTABLECOUNT_THRESHOLD = LoadPropertiesFile.getProperty("liveSSTableCountThreshold");
	private static final String ALLMEMTABLESLIVEDATASIZE_THRESHOLD = LoadPropertiesFile.getProperty("allMemtablesLiveDataSizeThreshold");
	private static final String READLATENCY_THRESHOLD = LoadPropertiesFile.getProperty("readLatencyThreshold");
	private static final String WRITELATENCY_THRESHOLD = LoadPropertiesFile.getProperty("writeLatencyThreshold");


	/**
	 * Create the output html file containing charts for the passed metrics.
	 *
	 * @param passedMetricHash A hash of metrics and values.
	 * @throws IOException
	 */
	public static void createChartHTML(SortedMap <String, String> passedMetricHash) throws IOException {
		logger.info("================================================================================");
		logger.info("  Starting Chart.createChartHTML...");

		String graphDir = LoadPropertiesFile.getProperty("graphDir");
		String graphTitle = "TestScreen_" + CURRENT_DATE;
		String htmlFile = graphDir + graphTitle + ".html";

		File file = new File(htmlFile);
		Files.deleteIfExists(file.toPath());

		PrintWriter printWriter = new PrintWriter(file);

		String liveSSTableCountString = "";
		String allMemtablesLiveDataSizeString = "";
		String readLatency95thPercentileString = "";
		String writeLatency95thPercentileString = "";

		// Iterate over the returned metrics results.
		for (Map.Entry<String, String> entry : passedMetricHash.entrySet()) {
			String timestamp = entry.getKey();
			String readMetricHashValue = entry.getValue();

			String[] metrics = readMetricHashValue.split(",");

			String[] datetime = timestamp.split(" ");
			String date = datetime[0].replaceAll("-", ",");
			String time = datetime[1].replaceAll(":", ",");
			time = time.substring(0, time.indexOf('.'));

			String formattedDate = "new Date(" + date + "," + time + ")";

			String liveSSTableCount = metrics[0];
			String allMemtablesLiveDataSize = metrics[1];
			String readLatency95thPercentile = metrics[2];
			String writeLatency95thPercentile = metrics[3];

			liveSSTableCountString += "[" + formattedDate + ", " + liveSSTableCount + ", " + LIVESSTABLECOUNT_THRESHOLD + "], ";
			allMemtablesLiveDataSizeString += "[" + formattedDate + ", " + allMemtablesLiveDataSize + ", " + ALLMEMTABLESLIVEDATASIZE_THRESHOLD +  "], ";
			readLatency95thPercentileString += "[" + formattedDate + ", " + readLatency95thPercentile + ", " + READLATENCY_THRESHOLD +  "], ";
			writeLatency95thPercentileString += "[" + formattedDate + ", " + writeLatency95thPercentile + ", " + WRITELATENCY_THRESHOLD +  "], ";
		}

		logger.debug("liveSSTableCountString");
		logger.debug(liveSSTableCountString);
		logger.debug("allMemtablesLiveDataSizeString");
		logger.debug(allMemtablesLiveDataSizeString);
		logger.debug("readLatency95thPercentileString");
		logger.debug(readLatency95thPercentileString);
		logger.debug("writeLatency95thPercentileString");
		logger.debug(writeLatency95thPercentileString);

		// Some repeated values for the graphs
		String haxis = 	"		         hAxis: {" +
				"		         title: 'Time'," +
				"		         format: 'yyyy-MM-dd HH:mm:ss'" +
				"		         },";
		String colors = "		         backgroundColor: '#f1f8e9'," +
			"		         colors: ['#AB0D06', '#007329'],";

		// This is very ugly, but it gets the job done.
		String htmlText = "<html>" +
				"		   <head>" +
				"		      <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" +
				"		      <script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>" +
				"		      <style>" +
				"		      pre code {" +
				"		        background-color: #eee;" +
				"		        border: 1px solid #999;" +
				"		        display: block;" +
				"		        padding: 20px;" +
				"		      }" +
				"		      </style>" +
				"		      <script type=\"text/javascript\">" +
				"		         google.charts.load('current', {packages: ['corechart', 'line']});" +
				"		         google.charts.setOnLoadCallback(drawBackgroundColor);" +
				"" +
				"		         function drawBackgroundColor() {" +
				// ------------------------------------------------------------------------------------------------------------------
				// LiveSSTableCount graph section
				"		         var dateFormatter = new google.visualization.DateFormat({pattern: 'yyyy-MM-dd HH:mm:ss'});" +
				"" +
				"		         var liveSSTableCountData = new google.visualization.DataTable();" +
				"		         liveSSTableCountData.addColumn('datetime', 'X');" +
				"		         liveSSTableCountData.addColumn('number', 'LiveSSTableCount');" +
				"		         liveSSTableCountData.addColumn('number', 'Threshold');" +
				"" +
				"		         liveSSTableCountData.addRows([" +
				liveSSTableCountString + "" +
				"		         ]);" +
				"" +
				"		         var liveSSTableCountOptions = {" +
				haxis +
				"		         vAxis: {" +
				"		         title: 'The number of live sorted string tables for this table'" +
				"		         }," +
				colors +
				"		         };" +
				"" +
				"		         var liveSSTableCountChart = new google.visualization.LineChart(document.getElementById('liveSSTableCountChart_div'));" +
				"		         liveSSTableCountChart.draw(liveSSTableCountData, liveSSTableCountOptions);" +
				"" +
				// ------------------------------------------------------------------------------------------------------------------
				// AllMemtablesLiveDataSize graph section
				"		         var allMemtablesLiveDataSizeData = new google.visualization.DataTable();" +
				"		         allMemtablesLiveDataSizeData.addColumn('datetime', 'X');" +
				"		         allMemtablesLiveDataSizeData.addColumn('number', 'AllMemtablesLiveDataSize');" +
				"		         allMemtablesLiveDataSizeData.addColumn('number', 'Threshold');" +
				"" +
				"		         allMemtablesLiveDataSizeData.addRows([" +
				allMemtablesLiveDataSizeString + "" +
				"		         ]);" +
				"" +
				"		         var allMemtablesLiveDataSizeOptions = {" +
				haxis +
				"		         vAxis: {" +
				"		         title: 'Total amount of live data stored in the memtables'" +
				"		         }," +
				colors +
				"		         };" +
				"" +
				"		         var allMemtablesLiveDataSizeChart = new google.visualization.LineChart(document.getElementById('allMemtablesLiveDataSizeChart_div'));" +
				"		         allMemtablesLiveDataSizeChart.draw(allMemtablesLiveDataSizeData, allMemtablesLiveDataSizeOptions);" +
				"" +
				// ------------------------------------------------------------------------------------------------------------------
				// ReadLatency95thPercentile graph section
				"		         var readLatency95thPercentileData = new google.visualization.DataTable();" +
				"		         readLatency95thPercentileData.addColumn('datetime', 'X');" +
				"		         readLatency95thPercentileData.addColumn('number', 'ReadLatency95thPercentile');" +
				"		         readLatency95thPercentileData.addColumn('number', 'Threshold');" +
				"" +
				"		         readLatency95thPercentileData.addRows([" +
				readLatency95thPercentileString + "" +
				"		         ]);" +
				"" +
				"		         var readLatency95thPercentileOptions = {" +
				haxis +
				"		         vAxis: {" +
				"		         title: 'Read latency in millis'" +
				"		         }," +
				colors +
				"		         };" +
				"" +
				"		         var readLatency95thPercentileChart = new google.visualization.LineChart(document.getElementById('readLatency95thPercentileChart_div'));" +
				"		         readLatency95thPercentileChart.draw(readLatency95thPercentileData, readLatency95thPercentileOptions);" +
				"" +
				// ------------------------------------------------------------------------------------------------------------------
				// WriteLatency95thPercentile graph section
				"		         var writeLatency95thPercentileData = new google.visualization.DataTable();" +
				"		         writeLatency95thPercentileData.addColumn('datetime', 'X');" +
				"		         writeLatency95thPercentileData.addColumn('number', 'WriteLatency95thPercentile');" +
				"		         writeLatency95thPercentileData.addColumn('number', 'Threshold');" +
				"" +
				"		         writeLatency95thPercentileData.addRows([" +
				writeLatency95thPercentileString + "" +
				"		         ]);" +
				"" +
				"		         var writeLatency95thPercentileOptions = {" +
				haxis +
				"		         vAxis: {" +
				"		         title: 'Write latency in millis'" +
				"		         }," +
				colors +
				"		         };" +
				"" +
				"		         var writeLatency95thPercentileChart = new google.visualization.LineChart(document.getElementById('writeLatency95thPercentileChart_div'));" +
				"		         writeLatency95thPercentileChart.draw(writeLatency95thPercentileData, writeLatency95thPercentileOptions);" +
				"" +
				"		         }" +
				"		      </script>" +
				"		   </head>" +
				"		   <body>" +
				"		      <H1>" + graphTitle + "</H1>" +
				"		      <p><br/></p>" +
				"		      <H2>Preamble</H2>" +
				"		      <p><br/></p>" +
				"             <p>cassandra-stress was run without the use of a custom yaml file, and in mixed mode for read/write operations. Because of this, the following mBean Values were totaled for the output below:<br/>" +
				"             <pre>" +
				"               <code>" +
				"org.apache.cassandra.metrics:keyspace=keyspace1,name=LiveSSTableCount,scope=counter1,type=ColumnFamily<br>" +
				"org.apache.cassandra.metrics:keyspace=keyspace1,name=LiveSSTableCount,scope=standard1,type=ColumnFamily" +
				"               </code>" +
				"             </pre>" +
				"And:<br>" +
				"             <pre>" +
				"               <code>" +
				"org.apache.cassandra.metrics:keyspace=keyspace1,name=AllMemtablesLiveDataSize,scope=counter1,type=ColumnFamily<br>" +
				"org.apache.cassandra.metrics:keyspace=keyspace1,name=AllMemtablesLiveDataSize,scope=standard1,type=ColumnFamily" +
				"               </code>" +
				"             </pre></p>" +
				"		      <p><br/></p>" +
				"             <p>The requested metric &quot;o.a.c.m.ColumnFamily.keyspace.columnfamily.AllMemTablesDataSize&quot; was renamed in 2.1 from AllMemtablesDataSize to AllMemtablesLiveDataSize. The metric available in the current release was used (AllMemtablesLiveDataSize).<br/>" +
				"		      <p><br/></p>" +
				"             <p>The requested metric &quot;o.a.c.m.ClientRequest.Write.95thPercentile&quot; appears to be incomplete. It was inferred that the Latency was being requested:" +
				"             <pre>" +
				"               <code>" +
				"org.apache.cassandra.metrics:name=Latency,scope=Write,type=ClientRequest" +
				"               </code>" +
				"             </pre></p>" +
				"		      <p><br/></p>" +
				"             <p>To compliment the Write 95thPercentile metric, it was decided to include the corresponding Read metric:<br>" +
				"             <pre>" +
				"               <code>" +
				"org.apache.cassandra.metrics:name=Latency,scope=Read,type=ClientRequest" +
				"               </code>" +
				"             </pre></p>" +
				"		      <p><br/></p>" +
				"             <hr>" +
				// ------------------------------------------------------------------------------------------------------------------
				// LiveSSTableCount graph section
				"		      <p><H3>LiveSSTableCount</H3></p>" +
				"		      <div id=\"liveSSTableCountChart_div\" style=\"width: 900px; height: 500px\"></div>" +
				"		      <p>The number of live SSTables (sorted string tables) on disk for this table.</p>" +
				"		      <p>With writes and deletes the SSTables will increase with regular operations as the memtables are flushed and written to disk, and will decrease with compaction. Compaction will merge multiple old SSTables into a new one, saving disk space in the process.</p>" +
				"             <hr>" +
				// ------------------------------------------------------------------------------------------------------------------
				// AllMemtablesLiveDataSize graph section
				"		      <p><H3>AllMemtablesLiveDataSize</H3></p>" +
				"		      <div id=\"allMemtablesLiveDataSizeChart_div\" style=\"width: 900px; height: 500px\"></div>" +
				"		      <p>Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead.<br/></p>" +
				"		      <p>This graph will help monitor data size in memory, and load.</p>" +
				"		      <p>Cassandra writes are first written to the commit log and to the memtables. When the size of the memtabes hit a limit, they are flushed to SSTables and the commit log purges its data.</p>" +
				"             <hr>" +
				// ------------------------------------------------------------------------------------------------------------------
				// ReadLatency95thPercentile graph section
				"		      <p><H3>ReadLatency95thPercentile</H3></p>" +
				"		      <div id=\"readLatency95thPercentileChart_div\" style=\"width: 900px; height: 500px\"></div>" +
				"		      <p>95% of the time the latency was less than the number displayed in the column.<br/></p>" +
				"		      <p>Spikes in latency indicate disk or i/o issues during the test. Possible resolutions include tuning, addition of disk space, or addition of nodes. The goal is to have consistent levels. High latency indicates a bottleneck that restricts the system&apos;s bandwidth.</p>" +
				"             <hr>" +
				// ------------------------------------------------------------------------------------------------------------------
				// WriteLatency95thPercentile graph section
				"		      <p><H3>WriteLatency95thPercentile</H3></p>" +
				"		      <div id=\"writeLatency95thPercentileChart_div\" style=\"width: 900px; height: 500px\"></div>" +
				"		      <p>95% of the writes in the stress test experienced this latency value.<br/></p>" +
				"		      <p>Spikes in latency indicate disk or i/o issues during the test. Possible resolutions include tuning, addition of disk space, or addition of nodes.  The goal is to have consistent levels. Spikes are expected during write operations to SSTables.</p>" +
				"		   </body>" +
				"		</html>";

		printWriter.println(htmlText);
		printWriter.close();

	}

}
