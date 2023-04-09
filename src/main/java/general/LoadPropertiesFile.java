package general;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoadPropertiesFile {
	
    private LoadPropertiesFile() { 
    } 
	
    private static final Logger logger = LogManager.getLogger(Class.class.getName());
	/**
	 * @param key The name of the parameter key.
	 */
	public static String getProperty(String key) {
		logger.debug("================================================================================");
		logger.debug("  Starting LoadPropertiesFile.getProperty '" + key + "'...");

		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("config.properties");

			// load a properties file
			prop.load(input);

		} catch (IOException io) {
			logger.catching(io);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.catching(e);
				}
			}
		}

		return prop.getProperty(key);
	}

}