package jmx_logger_qa_screen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogPrinterTest {
	
	private static final Logger logger = LogManager.getLogger(Class.class.getName());

    public static void main(String[] args) {
    	
		logger.info("================================================================================");
		logger.info("  Starting LogPrinterTest.main...");
    	
        logger.trace("Trace message");
        logger.debug("Debug message");
        logger.info("Info message");
        logger.warn("Warn message");
        logger.error("Error message");
    }
}