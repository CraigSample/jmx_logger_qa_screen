package general;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Miscellaneous {

	private static final Logger logger = LogManager.getLogger(Class.class.getName());


	/**
	 * Check if the passed metric string value is a positive number.
	 * Return the value as a double, returning 0 if the passed string is not a number or is a negative value.
	 *
	 * @param passedStringValue The string value to check.
	 * @param passedMetricName The name of the metric being checked.
	 * @param timestamp The timestamp of the given metric.
	 * @return Double The parsed value.
	 */
	public static Double convertStringValue(String passedStringValue, String passedMetricName, String timestamp) {
		logger.debug("================================================================================");
		logger.debug("  Starting Miscellaneous.convertStringValue for '" + passedMetricName + "'...");

		double tempValue = 0;

		if (isNumber(passedStringValue)) {
			tempValue = Double.parseDouble(passedStringValue);

			if (tempValue < 0) {
				logger.error("Timestamp " + timestamp + " value " + passedMetricName + " '" + passedStringValue + "' is negative!");
			}
		} else {
			// If we received an invalid number, 0 will be returned.
			logger.error("Timestamp " + timestamp + " value " + passedMetricName + " '" + passedStringValue + "' is not a valid number!");
		}

		return tempValue;
	}


	/**
	 * Report how long we have been waiting for the given name based upon the given timestamp.
	 *
	 * @param passedStartTime The timestamp of the given name.
	 * @param passedName The given name.
	 */
	public static void reportWaitTime(Long passedStartTime, String passedName) {
		logger.debug("================================================================================");
		logger.debug("  Starting Miscellaneous.reportWaitTime for '" + passedName + "'...");

		long currentTime = System.currentTimeMillis();

		long timeDifference = (currentTime - passedStartTime);
		int elapsedSeconds = (int) (timeDifference / 1000) % 60 ;

		int modulus30 = (int) (elapsedSeconds % 30);

		if (modulus30 == 0) {

			long secondsInMilli = 1000;
			long minutesInMilli = secondsInMilli * 60;
			long hoursInMilli = minutesInMilli * 60;

			long elapsedHours = timeDifference / hoursInMilli;
			timeDifference = timeDifference % hoursInMilli;

			long elapsedMinutes = timeDifference / minutesInMilli;

			logger.info(String.format("Waiting on " + passedName + " for: %02d hours, %02d min, %02d sec", elapsedHours, elapsedMinutes, elapsedSeconds));
		}
	}


	/**
	 * Get a range of values in the passed map between the given min and max int values.
	 *
	 * @param min The minimum value for the range.
	 * @param max The maximum value for the range.
	 * @param passedSortedMap The passed data map.
	 * @return SortedMap<String, String> The data in the desired range.
	 */
	public static SortedMap<String, String> getRangeOfSortedMapEntries(int min, int max,
			SortedMap<String, String> passedSortedMap) {
		logger.debug("================================================================================");
		logger.debug("  Starting Miscellaneous.getRangeOfSortedMapEntries '" + min + "." + max + "'...");

		int count = 0;

		TreeMap<String, String> subMap = new TreeMap<String, String>();
		for (Map.Entry<String, String> entry : passedSortedMap.entrySet()) {

			if ((count >= min) && (count <= (max - 1))) {
				subMap.put(entry.getKey(), entry.getValue());
			}

			if (count > max) {
				break;
			}
			count++;

		}
		return subMap;
	}


	/**
	 * Check if the given string value is a number or not.
	 *
	 * @param str The given string value.
	 * @return boolean Is the passed string value a number?
	 */
	public static boolean isNumber(String str) {
		logger.debug("================================================================================");
		logger.debug("  Starting Miscellaneous.isNumber for '" + str + "'...");

	    try {
	        @SuppressWarnings("unused")
			double v = Double.parseDouble(str);
	        return true;
	    } catch (NumberFormatException nfe) {
	    	logger.error(str + " is not a number!");
	    }
	    return false;
	}


	/**
	 * Convert the passed metric hash values from string to numbers.
	 *
	 * @param readMetricHash The metrics to be converted.
	 * @return SortedMap<String, String> The converted metrics.
	 */
	public static SortedMap<String, String> convertMetricHash(SortedMap <String, String> readMetricHash) {
		logger.info("================================================================================");
		logger.info("  Starting Miscellaneous.convertMetricHash...");

		TreeMap<String, String> validatedMetricHash = new TreeMap<>();

		int liveSSTableCountTotal = 0;
		int allMemtablesLiveDataSizeTotal= 0;
		double readLatency95thPercentileTotal = 0;
		double writeLatency95thPercentileTotal = 0;

		// Iterate over the returned metrics results.
		for (Map.Entry<String, String> entry : readMetricHash.entrySet()) {
			String timestamp = entry.getKey();
			String readMetricHashValue = entry.getValue();

			String[] metrics = readMetricHashValue.split(",");

			// Cast the string values to int/double to validate that the values are valid (numeric & not negative).
			// If a value is not numeric, zero will be used in its place.
			int liveSSTableCount = Miscellaneous.convertStringValue(metrics[0], "liveSSTableCount", timestamp).intValue();
			int allMemtablesLiveDataSize = Miscellaneous.convertStringValue(metrics[1], "allMemtablesLiveDataSize", timestamp).intValue();
			double readLatency95thPercentile = Miscellaneous.convertStringValue(metrics[2], "readLatency95thPercentile", timestamp);
			double writeLatency95thPercentile = Miscellaneous.convertStringValue(metrics[3], "writeLatency95thPercentile", timestamp);

			logger.debug("timestamp                 : " + timestamp);
			logger.debug("liveSSTableCount          : " + liveSSTableCount);
			logger.debug("allMemtablesLiveDataSize  : " + allMemtablesLiveDataSize);
			logger.debug("readLatency95thPercentile: " + readLatency95thPercentile);
			logger.debug("writeLatency95thPercentile: " + writeLatency95thPercentile);

			liveSSTableCountTotal += liveSSTableCount;
			allMemtablesLiveDataSizeTotal += allMemtablesLiveDataSize;
			readLatency95thPercentileTotal += readLatency95thPercentile;
			writeLatency95thPercentileTotal += writeLatency95thPercentile;

			// Convert the values back to String to be returned in the validated hash.
			validatedMetricHash.put(timestamp, String.join(",", Integer.toString(liveSSTableCount), Integer.toString(allMemtablesLiveDataSize), Double.toString(readLatency95thPercentile), Double.toString(writeLatency95thPercentile)));
		}

		if (liveSSTableCountTotal == 0) {
			logger.error("liveSSTableCount is always zero in results set!");
		}

		if (allMemtablesLiveDataSizeTotal == 0) {
			logger.error("allMemtablesLiveDataSize is always zero in results set!");
		}

		if (readLatency95thPercentileTotal == 0) {
			logger.error("readLatency95thPercentile is always zero in results set!");
		}

		if (writeLatency95thPercentileTotal == 0) {
			logger.error("writeLatency95thPercentile is always zero in results set!");
		}

		return validatedMetricHash;
	}

	private Miscellaneous() {
	}

}
