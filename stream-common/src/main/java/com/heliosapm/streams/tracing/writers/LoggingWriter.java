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
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.RollingRandomAccessFileManager;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
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
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;

/**
 * <p>Title: LoggingWriter</p>
 * <p>Description: A logging writer that writes a minimally formatted log4j2 log entry for each metric</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.LoggingWriter</code></p>
 */

public class LoggingWriter extends AbstractMetricWriter {
	/** The logger used to write the metrics */
	protected Logger metricLog = null;
	
	protected Appender appender = null;
	
	/** The JVM's temp directory */
	public static final File TMP = new File(System.getProperty("java.io.tmpdir"));
	/** The JVM's PID */
	public static final String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	
	/** The config key for the logger name to use */
	public static final String CONFIG_LOGGER_NAME = "metricwriter.logging.logname";
	/** The config key for the logger file to write to */
	public static final String CONFIG_FILE_NAME = "metricwriter.logging.filename";
	/** The default logger directory to write to */
	public static final String DEFAULT_DIR_NAME = new File(TMP, "stream-metrics").getAbsolutePath();
	/** The default logger file to write to */
	public static final String DEFAULT_FILE_NAME = new File(new File(DEFAULT_DIR_NAME), "streams.logfile." + PID + ".log").getAbsolutePath();

	/** The config key for the logger file roll name format */
	public static final String CONFIG_ROLL_PATTERN = "metricwriter.logging.rollformat";
	/** The default logger file roll name format */
	public static final String DEFAULT_ROLL_PATTERN = "-%d{MM-dd-yyyy-HH}.gz";
	
	/** The config key for the log entry prefix leading each logged line */
	public static final String CONFIG_ENTRY_PREFIX = "metricwriter.logging.entryprefix";
	/** The default log entry prefix leading each logged line */
	public static final String DEFAULT_ENTRY_PREFIX = "";
	
	/** The config key for the log entry suffix trailing each logged line */
	public static final String CONFIG_ENTRY_SUFFIX = "metricwriter.logging.entrysuffix";
	/** The default log entry suffix leading each logged line */
	public static final String DEFAULT_ENTRY_SUFFIX = "";
	
	/** The config key for the flag indicating if the rolling file appender is a random access file */
	public static final String CONFIG_RA_FILE = "metricwriter.logging.ra";
	/** The default random access file enablement */
	public static final boolean DEFAULT_RA_FILE = true;
	
	/** Prefix indicating a log entry is a metric */
	public static final String METRIC_PREFIX = "M:";
	/** Prefix indicating a log entry is an event */
	public static final String EVENT_PREFIX = "E:";
	
	/** The utf8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/**
	 * Creates a new LoggingWriter
	 */
	public LoggingWriter() {
		super(false);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(Properties config) {				
		
		final String loggerName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_LOGGER_NAME, null, config);
		
		if(loggerName==null || !LogManager.getContext(true).hasLogger(loggerName)) {
			/* 
			 * ===================================================
			 * FIXME:  this is super ugly
			 * ===================================================
			 * TODO:
			 *  - log4j2 async appender
			 *  - low gc message objects
			 */
			final String entryPrefix = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ENTRY_PREFIX, DEFAULT_ENTRY_PREFIX, config);
			final String entrySuffix = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ENTRY_SUFFIX, DEFAULT_ENTRY_SUFFIX, config);
			final boolean randomAccessFile = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_RA_FILE, DEFAULT_RA_FILE, config);
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
	        PatternLayout layout= PatternLayout.createLayout(entryPrefix + "%m" + entrySuffix + "%n" , null, loggingConfig, null, UTF8, false, false, null, null);
	        
	        
	        final DefaultRolloverStrategy strategy = DefaultRolloverStrategy.createStrategy("10", "1", null, null, null, true, loggingConfig);
//	        final int lastIndex = fileName.lastIndexOf('.');
	        final String format = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ROLL_PATTERN, DEFAULT_ROLL_PATTERN, config);
	        final StringBuilder b = new StringBuilder(fileName).append(format);
//	        if(lastIndex==-1) {
//	        	b.append(".").append(format);
//	        } else {
//	        	b.insert(lastIndex, format);
//	        }
	        final String rolledFileFormat = b.toString();
	        final TriggeringPolicy trigger = TimeBasedTriggeringPolicy.createPolicy("1", "true");
	        
	        if(randomAccessFile) {
	        	appender = createRollingRandomAccessFileAppender(fileName, loggingConfig, layout, strategy, rolledFileFormat, trigger);
	        } else {
	        	appender = createRollingFileAppender(fileName, loggingConfig, layout, strategy, rolledFileFormat, trigger);
	        }
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
	        metricLog = context.getLogger(getClass().getName() + "Logger");
		} else {
			metricLog = LogManager.getLogger(loggerName);
		}
	}
	
	
	/**
	 * Creates a new {@link RollingFileAppender}
	 * @param fileName The log file
	 * @param loggingConfig The logging config
	 * @param layout The layout
	 * @param strategy the rollover strategy
	 * @param rolledFileFormat The log file roll pattern
	 * @param trigger The roll trigger
	 * @return the new appender
	 */
	protected Appender createRollingRandomAccessFileAppender(final String fileName, Configuration loggingConfig, PatternLayout layout,
			final DefaultRolloverStrategy strategy, final String rolledFileFormat, final TriggeringPolicy trigger) {
        
        RollingRandomAccessFileManager fileManager = RollingRandomAccessFileManager.getRollingRandomAccessFileManager(
        		fileName, 
        		rolledFileFormat, 
        		true, 
        		true,
        		8192,
        		trigger, 
        		strategy, 
        		null, 
        		layout      		
        		);
        trigger.initialize(fileManager);
		
		return RollingRandomAccessFileAppender.createAppender(
				fileName, 						// file name
				rolledFileFormat, 				// rolled file name pattern
				"true", 						// append
				getClass().getSimpleName(), 	// appender name
				"true", 						// immediate flush
				"8192", 						// buffer size				
				trigger, 						// triggering policy
				strategy,						// rollover strategy
				layout, 						// layout
				null, 							// filter
				"true",							// ignore exceptions 
				null, 							// advertise
				null, 							// advertise uri
				loggingConfig);					// config
	}
	

	/**
	 * Creates a new {@link RollingFileAppender}
	 * @param fileName The log file
	 * @param loggingConfig The logging config
	 * @param layout The layout
	 * @param strategy the rollover strategy
	 * @param rolledFileFormat The log file roll pattern
	 * @param trigger The roll trigger
	 * @return the new appender
	 */
	protected Appender createRollingFileAppender(final String fileName, Configuration loggingConfig, PatternLayout layout,
			final DefaultRolloverStrategy strategy, final String rolledFileFormat, final TriggeringPolicy trigger) {
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
		
		return RollingFileAppender.createAppender(
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
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doStart()
	 */
	@Override
	protected void doStart() {		
		appender.start();
//		notifyStarted();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doStop()
	 */
	@Override
	protected void doStop() {
		appender.stop();
//		notifyStopped();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(Collection<StreamedMetric> metrics) {
		for(StreamedMetric sm: metrics) {
			metricLog.info(StringHelper.fastConcat(METRIC_PREFIX, sm.toString()));
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		for(StreamedMetric sm: metrics) {
			metricLog.info(StringHelper.fastConcat(METRIC_PREFIX, sm.toString()));
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
					StreamedMetricValue smv = new StreamedMetricValue(System.currentTimeMillis(), Math.abs(r.nextInt(100) + r.nextDouble()), "foo.bar", AgentName.getInstance().defaultTags()).setValueType(ValueType.DELTA);
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

	@Override
	protected void startUp() throws Exception {
		appender.start();
		System.err.println("Appender Started");
		
	}

	@Override
	protected void shutDown() throws Exception {
		appender.stop();
		System.err.println("Appender Stopped");
		
	}
}
