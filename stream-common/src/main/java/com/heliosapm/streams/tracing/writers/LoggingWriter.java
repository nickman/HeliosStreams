/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package com.heliosapm.streams.tracing.writers;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.time.SystemClock;

/**
 * <p>Title: LoggingWriter</p>
 * <p>Description: A logging writer that writes a minimally formatted log4j2 log entry for each metric</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.LoggingWriter</code></p>
 */

public class LoggingWriter extends AbstractMetricWriter {
	/** The logger used to write the metrics */
	protected Logger log = null;
	
	protected Appender appender = null;
	
	private static final File TMP = new File(System.getProperty("java.io.tmpdir"));
	
	/** The config key for the logger name to use */
	public static final String CONFIG_LOGGER_NAME = "metricwriter.logging.logname";
	/** The config key for the logger file to write to */
	public static final String CONFIG_FILE_NAME = "metricwriter.logging.filename";
	/** The default logger directory to write to */
	public static final String DEFAULT_DIR_NAME = new File(TMP, "stream-metrics").getAbsolutePath();
	/** The default logger file to write to */
	public static final String DEFAULT_FILE_NAME = new File(new File(DEFAULT_DIR_NAME), "streams.logfile.out").getAbsolutePath();

	/** The config key for the logger file roll name format */
	public static final String CONFIG_ROLL_PATTERN = "metricwriter.logging.rollformat";
	/** The default logger file roll name format */
	public static final String DEFAULT_ROLL_PATTERN = "-%d{MM-dd-yyyy-HH-mm}";
	
	
	
	/** The utf8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/**
	 * Creates a new LoggingWriter
	 */
	public LoggingWriter() {
		super(false);
	}
	
	@Override
	public void configure(Properties config) {				final String loggerName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_LOGGER_NAME, null, config);
		if(loggerName==null || !LogManager.getContext(true).hasLogger(loggerName)) {
			/* 
			 * ===================================================
			 * FIXME:  this is super ugly
			 * ===================================================
			 * TODO:
			 *  - log4j2 async appender
			 *  - low gc message objects
			 */
			final String fileName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_FILE_NAME, DEFAULT_FILE_NAME, config);
			final File file = new File(fileName);
			final File dir = file.getParentFile();
			if(dir.exists()) {
				if(!dir.isDirectory()) throw new IllegalArgumentException("The logging directory is a file [" + dir + "]");
			} else {
				if(!dir.mkdirs()) throw new IllegalArgumentException("Cannot create the logging directory [" + dir + "]");
			}
			LoggerContext context= (LoggerContext) LogManager.getContext();
	        Configuration loggingConfig = context.getConfiguration();
	        PatternLayout layout= PatternLayout.createLayout("%m%n", null, loggingConfig, null, UTF8, false, false, null, null);
	        
	        
	        final DefaultRolloverStrategy strategy = DefaultRolloverStrategy.createStrategy("10", "1", null, null, null, true, loggingConfig);
	        final int lastIndex = fileName.lastIndexOf('.');
	        final String format = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ROLL_PATTERN, DEFAULT_ROLL_PATTERN, config);
	        final StringBuilder b = new StringBuilder(fileName);
	        if(lastIndex==-1) {
	        	b.append(".").append(format);
	        } else {
	        	b.insert(lastIndex, format);
	        }
	        final String rolledFileFormat = b.toString();
	        final TriggeringPolicy trigger = TimeBasedTriggeringPolicy.createPolicy("1", "true");
	        
	        RollingFileManager fileManager = RollingFileManager.getFileManager(
	        		fileName, 
	        		rolledFileFormat, 
	        		false, 
	        		true,
	        		trigger, 
	        		strategy, 
	        		null, 
	        		layout, 
	        		8192,
	        		true
	        		);
	        trigger.initialize(fileManager);	        
	        appender = RollingFileAppender.createAppender(
	        		fileName, 						// file name
	        		rolledFileFormat, 				// rolled file name pattern
	        		"true", 						// append
	        		getClass().getSimpleName(), 	// appender name
	        		"true", 						// buffered io
	        		"8192", 						// buffer size
	        		"true", 						// immediate flush	
	        		trigger, 						// triggering policy
	        		strategy,						// rollover strategy
	        		layout, 						// layout
	        		null, 							// filter
	        		"true",							// ignore exceptions 
	        		null, 							// advertise
	        		null, 							// advertise uri
	        		loggingConfig);					// config
	        
	        loggingConfig.addAppender(appender);
	        AppenderRef ref = AppenderRef.createAppenderRef(getClass().getSimpleName(), Level.INFO, null);
	        AppenderRef[] refs = new AppenderRef[] { ref };
	        LoggerConfig loggerConfig = LoggerConfig.createLogger(
	        		false, 
	        		Level.INFO, 
	        		getClass().getSimpleName() + "Logger",  
	        		"false", refs, null, loggingConfig, null);
	        loggerConfig.addAppender(appender, Level.INFO, null);
	        
	        loggingConfig.addLogger(getClass().getSimpleName() + "Logger", loggerConfig);
	        context.updateLoggers();	        
	        org.apache.logging.log4j.core.Logger xlogger = 	context.getLogger(getClass().getName() + "Logger");
	        for(Appender app: xlogger.getAppenders().values()) {
	        	xlogger.removeAppender(app);
	        }
	        xlogger.addAppender(appender);
	        log = context.getLogger(getClass().getName() + "Logger");
		} else {
			log = LogManager.getLogger(loggerName);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doStart()
	 */
	@Override
	protected void doStart() {		
		appender.start();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doStop()
	 */
	@Override
	protected void doStop() {
		appender.stop();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(Collection<StreamedMetric> metrics) {
		for(StreamedMetric sm: metrics) {
			log.info(sm.toString());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		for(StreamedMetric sm: metrics) {
			log.info(sm.toString());
		}
	}

	public static void main(String[] args) {
		log("LoggingWriter Test");
		final Random r = new Random(System.currentTimeMillis());
		LoggingWriter lw = new LoggingWriter();
		lw.configure(null);
		lw.doStart();
		try {
			//for(int i = 0; i < 100; i++) {
			while(true) {
				for(int i = 0; i < 100; i++) {
					StreamedMetricValue smv = new StreamedMetricValue(System.currentTimeMillis(), Math.abs(r.nextInt(100) + r.nextDouble()), "foo.bar", AgentName.defaultTags()).setValueType(ValueType.DELTA);
					lw.onMetrics(smv);
				}
				log("Loop...");
				SystemClock.sleep(5000);
			}			

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			lw.doStop();
		}
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
}
