package jmx_logger_qa_screen;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jmx.RunExternalJmxTerm;

public class RunExternalJmxTermTest {

	private static final Logger logger = LogManager.getLogger(Class.class.getName());


	/**
	 * Test running the external JMX term process.
	 *
	 * @param args Main passed arguments.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		logger.info("================================================================================");
		logger.info("  Starting RunExternalJmxTermTest.main...");

		RunExternalJmxTerm.runExternalJmxTerm("keyspace1");

		RunExternalJmxTerm.close();
	}

}
