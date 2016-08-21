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
package com.heliosapm.streams.metrics.processors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;
import org.springframework.jmx.support.MetricType;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: AbstractStreamedMetricProcessor</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor</code></p>
 * @param <K> The key type
 * @param <V> The value type
 */
@ManagedResource
public abstract class AbstractStreamedMetricProcessor<K,V> implements Processor<K, V>, BeanNameAware, SelfNaming {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The application id of the context */
	protected String applicationId = null;
	/** The value type processed by this processor */
	protected final ValueType valueType;
	/** The names of state stores used by this processor */
	protected final String[] stateStoreNames;
	/** The injected processor context */
	private ProcessorContext context = null;
	/** The punctuation period */
	protected final long period;
	/** The state stores allocated for this processor */
	protected Map<String, StateStore> stateStores = new HashMap<String, StateStore>();
	/** The processing timer */
	//protected final Timer timer = SharedMetricsRegistry.getInstance().timer("StreamedMetricProcessor." + getClass().getSimpleName() + ".processed");
	protected final Timer timer = SharedMetricsRegistry.getInstance().timer(getClass().getSimpleName() + ".processed");
	/** The dropped metric counter */
	//protected final Counter dropCounter = SharedMetricsRegistry.getInstance().counter("StreamedMetricProcessor." + getClass().getSimpleName() + ".dropped");
	protected final Counter dropCounter = SharedMetricsRegistry.getInstance().counter(getClass().getSimpleName() + ".dropped");
	/** The forwarded message counter */
	private final Counter forwardCounter = SharedMetricsRegistry.getInstance().counter(getClass().getSimpleName() + ".forwarded");
	/** A timed cache of the timer's snapshot */
	protected final CachedGauge<Snapshot> timerSnapshot = new CachedGauge<Snapshot>(15, TimeUnit.SECONDS) {
		@Override
		protected Snapshot loadValue() {
			return timer.getSnapshot();
		}
	};
	/** A counter for failed forwards */
	private final LongAdder forwardFailures = new LongAdder();
	
	// ================
	// Forwarding
	// ================
	/** The maximum number of forwarded messages without a commit */
	protected final int maxForwardsWithoutCommit;
	/** The number of uncommited forwards */
	protected final AtomicInteger uncomittedForwards = new AtomicInteger(0);
	/** CAS lock on commit */
	protected final AtomicBoolean inCommit = new AtomicBoolean(false);
	
	
	
	// ================
	
	/** The processor's bean name */
	protected String beanName = null;
	/** The processor's JMX ObjectName name */
	protected ObjectName objectName = null;
	

	/**
	 * Creates a new AbstractStreamedMetricProcessor
	 * @param valueType The value type this processor supplies stream processing for
	 * @param period The punctuation period (ignored if less than 1)
	 * @param maxForwards The max forwards without a commit
	 * @param stateStoreNames The names of the state stores used by this processor
	 */
	protected AbstractStreamedMetricProcessor(final ValueType valueType, final long period, final int maxForwards, final String...stateStoreNames) {
		this.valueType = valueType;
		this.period = period;
		this.stateStoreNames = stateStoreNames;
		this.maxForwardsWithoutCommit = maxForwards; 
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#init(org.apache.kafka.streams.processor.ProcessorContext)
	 */
	@Override
	public void init(final ProcessorContext context) {
		this.context = context;
		if(stateStoreNames!=null && stateStoreNames.length != 0) {
			for(String ssName: stateStoreNames) {
				StateStore store = context.getStateStore(ssName);
				stateStores.put(ssName, store);
			}
		}
		if(period > 0L) {
			context.schedule(period);
		}
		applicationId = context.applicationId();
	}
	
	
	
	/**
	 * Returns the named state store
	 * @param name the name of the state store
	 * @return the named state store or null if no state store was bound to the passed name
	 */
	public StateStore getStateStore(final String name) {
		return stateStores.get(name);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#process(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void process(final K key, final V value) {
		log.debug("Processing Metric [{}]", key);
		final Context ctx = timer.time();
		if(doProcess(key, value)) {
			ctx.stop();
		} else {
			dropCounter.inc();
		}
	}
	
	/**
	 * Processes the passed streamed metric
	 * @param key The streamed metric key
	 * @param value The streamed metric to process
	 * @return true if the metric was processed, false otherwise
	 */
	protected abstract boolean doProcess(final K key, final V value);

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		/* No Op */
	}
	
	/**
	 * Commits the context
	 */
	protected final void commit() {
		for(; ;) {
			if(inCommit.compareAndSet(false, true)) {
				try {
					final int current = uncomittedForwards.get(); 
					if(current > 0) {
						context.commit();
						forwardCounter.inc(
							uncomittedForwards.getAndSet(0)
						);
						log.info("Comitted [{}] messages", current);
						return;
					}
				} finally {
					inCommit.set(false);					
				}
				break;
			}			
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#close()
	 */
	@Override
	public void close() {
//		log.info(">>>>>  Stopping [{}]...", getClass().getSimpleName());
//		if(!stateStores.isEmpty()) {
//			for(String key: new HashSet<String>(stateStores.keySet())) {
//				final StateStore store = stateStores.remove(key);
//				if(store!=null) {					
//					log.info("\tClosing Store [{}]...", store.name());
//					try { store.flush(); } catch (Exception x) {/* No Op */}
//					try { store.close(); } catch (Exception x) {/* No Op */}
//					log.info("\tStore Closed [{}].", store.name());
//				}
//			}
//		}
//		log.info("<<<<< Stopped [{}].", getClass().getSimpleName());
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.jmx.export.naming.SelfNaming#getObjectName()
	 */
	@Override
	@ManagedAttribute(description="This processor's JMX ObjectName")
	public ObjectName getObjectName() throws MalformedObjectNameException {
		objectName = JMXHelper.objectName("com.heliosapm.streams.metrics.processors:service=Processor,type=" + getClass().getSimpleName() + ",name=" + beanName);
		return objectName;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		this.beanName = name;
		
	}

	/**
	 * Returns the default assumed value type for metrics processed here
	 * @return the default assumed value type for metrics processed here
	 */
	@ManagedAttribute(description="The default assumed value type for metrics processed here")
	public String getValueType() {
		return valueType==null ? null : valueType.name();
	}

	/**
	 * Returns the names of the state stores used by this processor
	 * @return the names of the state stores used by this processor
	 */
	@ManagedAttribute(description="The names of the state stores used by this processor")
	public String[] getStateStoreNames() {
		return stateStoreNames;
	}

	/**
	 * Returns the punctuation period in ms.
	 * @return the punctuation period in ms.
	 */
	@ManagedAttribute(description="The punctuation period in ms.")
	public long getPuntuationPeriod() {
		return period;
	}

	/**
	 * Returns the number of dropped messages
	 * @return the number of dropped messages
	 */
	@ManagedAttribute(description="The number of dropped messages")
	public long getDropCount() {
		return dropCounter.getCount();
	}

	/**
	 * Returns the bean name
	 * @return the beanName
	 */
	@ManagedAttribute(description="The processor name")
	public String getProcessorName() {
		return beanName;
	}

	/**
	 * Returns the number of processed inbound messages
	 * @return the number of processed inbound messages
	 * @see com.codahale.metrics.Timer#getCount()
	 */
	@ManagedMetric(metricType=MetricType.COUNTER, category="MetricProcessors", description="The total number of processed inbound messages")
	public long getInboundCount() {
		return timer.getCount();
	}

	/**
	 * Returns the rate of processed inbound messages in the last 15 minutes
	 * @return the rate of processed inbound messages in the last 15 minutes
	 * @see com.codahale.metrics.Timer#getFifteenMinuteRate()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The rate of processed inbound messages in the last 15 minutes")
	public double getInboundRate15m() {
		return timer.getFifteenMinuteRate();
	}

	/**
	 * Returns the rate of processed inbound messages in the last 5 minutes
	 * @return the rate of processed inbound messages in the last 5 minutes
	 * @see com.codahale.metrics.Timer#getFiveMinuteRate()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The rate of processed inbound messages in the last 5 minutes")
	public double getInboundRate5m() {
		return timer.getFiveMinuteRate();
	}

	/**
	 * Returns the mean rate of processed inbound messages
	 * @return the mean rate of processed inbound messages
	 * @see com.codahale.metrics.Timer#getMeanRate()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The mean rate of processed inbound messages")
	public double getInboundMeanRate() {
		return timer.getMeanRate();
	}

	/**
	 * Returns the rate of processed inbound messages in the last minute
	 * @return the rate of processed inbound messages in the last minute
	 * @see com.codahale.metrics.Timer#getOneMinuteRate()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The rate of processed inbound messages in the last minute")
	public double getInboundRate1m() {
		return timer.getOneMinuteRate();
	}


	/**
	 * Returns the median time in ms to process inbound messages
	 * @return the median time in ms to process inbound messages
	 * @see com.codahale.metrics.Snapshot#getMedian()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms to process inbound messages", unit="ms")
	public double getInboundElapsedMedian() {
		return timerSnapshot.getValue().getMedian();
	}

	/**
	 * Returns the median time in ms at which 75% of inbound messages are processed
	 * @return the median time in ms at which 75% of inbound messages are processed
	 * @see com.codahale.metrics.Snapshot#get75thPercentile()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms at which 75% of inbound messages are processed", unit="ms")
	public double getInboundElapsed75Pct() {
		return timerSnapshot.getValue().get75thPercentile();
	}

	/**
	 * Returns the median time in ms at which 95% of inbound messages are processed
	 * @return the median time in ms at which 95% of inbound messages are processed
	 * @see com.codahale.metrics.Snapshot#get95thPercentile()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms at which 95% of inbound messages are processed", unit="ms")
	public double getInboundElapsed95Pct() {
		return timerSnapshot.getValue().get95thPercentile();
	}

	/**
	 * Returns the median time in ms at which 98% of inbound messages are processed
	 * @return the median time in ms at which 98% of inbound messages are processed
	 * @see com.codahale.metrics.Snapshot#get98thPercentile()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms at which 98% of inbound messages are processed", unit="ms")
	public double getInboundElapsed98Pct() {
		return timerSnapshot.getValue().get98thPercentile();
	}

	/**
	 * Returns the median time in ms at which 99% of inbound messages are processed
	 * @return the median time in ms at which 99% of inbound messages are processed
	 * @see com.codahale.metrics.Snapshot#get99thPercentile()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms at which 99% of inbound messages are processed", unit="ms")
	public double getInboundElapsed99Pct() {
		return timerSnapshot.getValue().get99thPercentile();
	}

	/**
	 * Returns the median time in ms at which 99.9% of inbound messages are processed
	 * @return the median time in ms at which 99.9% of inbound messages are processed
	 * @see com.codahale.metrics.Snapshot#get999thPercentile()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The median time in ms at which 99.9% of inbound messages are processed", unit="ms")
	public double getInboundElapsed999Pct() {
		return timerSnapshot.getValue().get999thPercentile();
	}

	/**
	 * Returns the max time in ms to process inbound messages
	 * @return the max time in ms to process inbound messages
	 * @see com.codahale.metrics.Snapshot#getMax()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The max time in ms to process inbound messages", unit="ms")
	public long getInboundElapsedMax() {
		return timerSnapshot.getValue().getMax();
	}

	/**
	 * Returns the mean time in ms to process inbound messages
	 * @return the mean time in ms to process inbound messages
	 * @see com.codahale.metrics.Snapshot#getMean()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The mean time in ms to process inbound messages", unit="ms")
	public double getInboundElapsedMean() {
		return timerSnapshot.getValue().getMean();
	}

	/**
	 * Returns the minimum time in ms to process inbound messages
	 * @return the minimum time in ms to process inbound messages
	 * @see com.codahale.metrics.Snapshot#getMin()
	 */
	@ManagedMetric(metricType=MetricType.GAUGE, category="MetricProcessors", description="The minimum time in ms to process inbound messages", unit="ms")
	public long getInboundElapsedMin() {
		return timerSnapshot.getValue().getMin();
	}

	/**
	 * Returns the count of forwarded messages
	 * @return the forwardCounter
	 */
	@ManagedMetric(metricType=MetricType.COUNTER, category="MetricProcessors", description="The count of forwarded messages")
	public long getForwardCount() {
		return forwardCounter.getCount();
	}

	/**
	 * Returns the max number of forwards without a commit
	 * @return the max number of forwards without a commit
	 */
	@ManagedAttribute(description="The max number of forwards without a commit")
	public int getMaxForwardsWithoutCommit() {
		return maxForwardsWithoutCommit;
	}

	/**
	 * Returns the number of forwards without a commit
	 * @return the uncomittedForwards
	 */
	@ManagedAttribute(description="The number of forwards without a commit")
	public int getUncomittedForwards() {
		return uncomittedForwards.get();
	}

	/**
	 * Returns the application id
	 * @return the application id
	 * @see org.apache.kafka.streams.processor.ProcessorContext#applicationId()
	 */
	@ManagedAttribute(description="The processor application id")
	public String getApplicationId() {
		return applicationId;
	}

	/**
	 * Returns the task id
	 * @return the task id
	 * @see org.apache.kafka.streams.processor.ProcessorContext#taskId()
	 */
	@ManagedAttribute(description="The processor task id")
	public String getTaskId() {
		return context.taskId().toString();
	}

	/**
	 * Returns the name of the key serde class
	 * @return the name of the key serde class
	 * @see org.apache.kafka.streams.processor.ProcessorContext#keySerde()
	 */
	@ManagedAttribute(description="The processor key serde type")
	public String getKeySerde() {
		return context.keySerde().getClass().getName();
	}

	/**
	 * Returns the name of the value serde class
	 * @return the name of the value serde class
	 * @see org.apache.kafka.streams.processor.ProcessorContext#valueSerde()
	 */
	@ManagedAttribute(description="The processor value serde type")
	public String getValueSerde() {
		return context.valueSerde().getClass().getName();
	}

	/**
	 * Returns this processor's state directory
	 * @return this processor's state directory
	 * @see org.apache.kafka.streams.processor.ProcessorContext#stateDir()
	 */
	@ManagedAttribute(description="The processor state directory")
	public String getStateDir() {
		return context.stateDir().getAbsolutePath();
	}
	
	/**
	 * Returns the total number of failed forwards
	 * @return the total number of failed forwards
	 */
	@ManagedAttribute(description="The total number of failed forwards")
	public long getFailedForwards() {
		return forwardFailures.sum();
	}

//	/**
//	 * @param store
//	 * @param loggingEnabled
//	 * @param stateRestoreCallback
//	 * @see org.apache.kafka.streams.processor.ProcessorContext#register(org.apache.kafka.streams.processor.StateStore, boolean, org.apache.kafka.streams.processor.StateRestoreCallback)
//	 */
//	public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
//		context.register(store, loggingEnabled, stateRestoreCallback);
//	}

	/**
     * Forwards a key/value pair to the downstream processors
     * @param key key
     * @param value value     * 
	 * @see org.apache.kafka.streams.processor.ProcessorContext#forward(java.lang.Object, java.lang.Object)
	 */
	protected final void forward(final K key, final V value) {
		if(key==null || value==null) {
			log.warn("KorV null [{}}:[{}]", key, value);
			return;
		}
		if(context!=null) {
			try {
				context.forward(key, value);
				if(uncomittedForwards.incrementAndGet() > maxForwardsWithoutCommit) {
					commit();
				}
			} catch (Exception ex) {
//				log.error("Forwarding failed", ex);
				forwardFailures.increment();
			}
		} else {
			log.warn("Context is null !!");
		}
	}

	/**
     * Forwards a key/value pair to one of the downstream processors designated by childIndex
     * @param key key
     * @param value value
     * @param childIndex index in list of children of this node
	 * @see org.apache.kafka.streams.processor.ProcessorContext#forward(java.lang.Object, java.lang.Object, int)
	 */
	protected final void forward(final K key, final V value, final int childIndex) {
		context.forward(key, value, childIndex);
		if(uncomittedForwards.incrementAndGet() > maxForwardsWithoutCommit) {
			commit();
		}		
	}

	/**
     * Forwards a key/value pair to one of the downstream processors designated by the downstream processor name
     * @param key key
     * @param value value
     * @param childName name of downstream processor
	 * @see org.apache.kafka.streams.processor.ProcessorContext#forward(java.lang.Object, java.lang.Object, java.lang.String)
	 */
	protected final void forward(final K key, final V value, final String childName) {
		context.forward(key, value, childName);
		if(uncomittedForwards.incrementAndGet() > maxForwardsWithoutCommit) {
			commit();
		}				
	}

	/**
     * Returns the topic name of the current input record; could be null if it is not
     * available (for example, if this method is invoked from the punctuate call)
     * @return the topic name
	 * @see org.apache.kafka.streams.processor.ProcessorContext#topic()
	 */
	@ManagedAttribute(description="The topic name of the current input record")
	public String getTopic() {
		return context.topic();
	}

	/**
     * Returns the partition id of the current input record; could be -1 if it is not
     * available (for example, if this method is invoked from the punctuate call)
     * @return the partition id
	 * @see org.apache.kafka.streams.processor.ProcessorContext#partition()
	 */
	@ManagedAttribute(description="The partition id of the current input record")
	public int getPartition() {
		return context.partition();
	}

	/**
     * Returns the offset of the current input record; could be -1 if it is not
     * available (for example, if this method is invoked from the punctuate call)
     * @return the offset
	 * @see org.apache.kafka.streams.processor.ProcessorContext#offset()
	 */
	@ManagedAttribute(description="The partition id of the current input record")
	public long getOffset() {
		return context.offset();
	}


}
