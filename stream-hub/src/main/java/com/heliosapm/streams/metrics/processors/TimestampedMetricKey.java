/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.metrics.processors;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.kafka.common.errors.SerializationException;

import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: TimestampedMetricKey</p>
 * <p>Description: A key representing the second based unix time and a metric key</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.TimestampedMetricKey</code></p>
 */

public class TimestampedMetricKey {
	/** The effective time window of this key in unit time*/
	protected volatile long[] timeWindow;
	/** The count of instances of metrics with this key */
	protected long count = 0;	
	/** The metric key */
	protected final String metricKey;
	/** The width of the window in seconds */
	protected final long windowWidth;
	
	
	/** Atomic field updated for the time window */
	private static final AtomicReferenceFieldUpdater<TimestampedMetricKey, long[]> TIME_WINDOW_UPDATER
	  	 = AtomicReferenceFieldUpdater.newUpdater(TimestampedMetricKey.class, long[].class, "timeWindow");
	
	/** The string character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** Placeholder TimestampedMetricKey */
	public static final TimestampedMetricKey PLACEHOLDER = new TimestampedMetricKey();
	
//	private static final int LONG_BYTES = 16;
	
	/**
	 * Creates a new TimestampedMetricKey
	 * @param unixTime The effective time of this key in unix time
	 * @param initialCount The initial count
	 * @param metricKey The metric key
	 * @param windowWidth the width of the time window in seconds
	 */
	public TimestampedMetricKey(final long unixTime, final long initialCount, final String metricKey, final long windowWidth) {		
		this.metricKey = metricKey;
		this.windowWidth = windowWidth;
		this.count = initialCount;
		this.timeWindow = windowRangeFromSec(unixTime, windowWidth);
	}
	
	private TimestampedMetricKey() {
		timeWindow = null;
		count = -1L;
		windowWidth = -1L;
		metricKey = null;
	}
	
	
//	private TimestampedMetricKey(final byte[] bytes) {
//		unixTime = deserialize(bytes, 0);
//		count = deserialize(bytes, 8);
//		metricKey = new String(bytes, LONG_BYTES, bytes.length-LONG_BYTES, UTF8);
//	}
//	
//	private byte[] toBytes() {
//		final byte[] sbytes = metricKey.getBytes(UTF8);
//		final byte[] ser = new byte[sbytes.length + LONG_BYTES];
//		serialize(unixTime, count, ser);
//		System.arraycopy(sbytes, 0, ser, LONG_BYTES, sbytes.length);
//		return ser;
//	}
	
	/**
	 * Resets the count to zero and sets the time window according to the passed ms timestamp
	 * @param newStartMs The new window start time in ms
	 * @param newCount The new count to start at
	 * @return An NVP of the the prior window end unix time and the effective count per second
	 */
	public NVP<Long, Double> reset(final long newStartMs, final long newCount) {
		final long[] priorWindow = TIME_WINDOW_UPDATER.getAndSet(this, windowRangeFromMs(newStartMs, windowWidth));
		final long priorCount = count;
		count = newCount;
		return new NVP<Long, Double>(priorWindow[1], calcRate(priorCount, windowWidth));
	}
	
	private static double calcRate(final double count, final double windowWidth) {
		if(count==0D || windowWidth==0D) return  0D;
		return count / windowWidth;
	}
		
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return metricKey + ":" + timeWindow[0] + ":" + count;
	}
	
	
	/**
	 * Returns the effective start time of this key as a unix time
	 * @return the time window start time
	 */
	public long getStartTime() {
		return timeWindow[0];
	}
	
	/**
	 * Returns the effective end time of this key as a unix time
	 * @return the time window end time
	 */
	public long getEndTime() {
		return timeWindow[1];
	}
	
 
	/**
	 * Returns the metric key
	 * @return the metricKey
	 */
	public String getMetricKey() {
		return metricKey;
	}
	
	/**
	 * Returns the metric instance count
	 * @return the metric instance count
	 */
	public long getCount() {
		return count;
	}
	
	
	public static long periodFromMs(final long time, final long windowSize) {
		final long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		final long mod = secs%windowSize;
		return secs - mod;
	}
	
	public static long periodFromSec(final long secs, final long windowSize) {
		final long mod = secs%windowSize;
		return secs - mod;
	}
	
	public static long[] windowRangeFromSec(final long secs, final long windowSize) {
		final long[] range = new long[2];
		range[0] =  secs - secs%windowSize;
		range[1] = range[0] + windowSize;
		return range;
	}
	
	public static long[] windowRangeFromMs(final long ms, final long windowSize) {
		final long[] range = new long[2];
		final long secs = TimeUnit.MILLISECONDS.toSeconds(ms);
		range[0] =  secs - secs%windowSize;
		range[1] = range[0] + windowSize;
		return range;
	}
	
	
	/**
	 * Indicates if the passed timestamp is in the same aggregation period
	 * @param mstime The ms timestamp
	 * @param count The number to increment by
	 * @return true if in the range, false otherwise
	 */
	public boolean isSameAggPeriodAs(final long mstime, final long count) {
		final long utime = periodFromMs(mstime, windowWidth);		
		if(periodFromMs(mstime, windowWidth)==timeWindow[0]) {
			this.count += count;
			return true;
		}
		return false;
	}
	
	public NVP<Long, Double> punctuate(final long timestamp) {
		if(count==0) return null;		
		NVP<Long, Double> nvp = new NVP<Long, Double>(timeWindow[1], calcRate(count, windowWidth));
		count = 0;
		TIME_WINDOW_UPDATER.set(this, windowRangeFromMs(timestamp, windowWidth));
		return nvp;
	}
	
	public void increment(final long count) {
		this.count += count;
	}
	
	/**
	 * Indicates if this TimestampedMetricKey is expired but not idle
	 * @param mstime The ms timestamp
	 * @param idleTimeout The idle timeout in seconds
	 * @return true if this TimestampedMetricKey is expired, false otherwise
	 */
	public boolean isExpired(final long mstime, final long idleTimeout) {
		final long utime = periodFromMs(mstime, windowWidth);		
		return utime > timeWindow[1] && utime < (timeWindow[0] + idleTimeout); 
	}
	
	/**
	 * Determines if this {@link TimestampedMetricKey} has been idle for at least the idle timeout
	 * @param mstime The current time in ms.
	 * @param idleTimeout The idle timeout in seconds
	 * @return true if idle, false otherwise
	 */
	public boolean isIdle(final long mstime, final long idleTimeout) {
		final long utime = periodFromMs(mstime, windowWidth);
		return utime >= (timeWindow[0] + idleTimeout);
	}
	
	
//	/**
//	 * <p>Title: TimestampedMetricKeySerializer</p>
//	 * <p>Description: Serializers for TimestampedMetric instances</p> 
//	 * <p>Company: Helios Development Group LLC</p>
//	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
//	 * <p><code>com.heliosapm.streams.metrics.processors.TimestampedMetric.TimestampedMetricKeySerializer</code></p>
//	 */
//	public static class TimestampedMetricKeySerializer implements Serializer<TimestampedMetricKey> {
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Serializer#configure(java.util.Map, boolean)
//		 */
//		@Override
//		public void configure(final Map<String, ?> configs, final boolean isKey) {
//			/* Nop Op */			
//		}
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Serializer#close()
//		 */
//		@Override
//		public void close() {
//			/* No Op */
//		}
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
//		 */
//		@Override
//		public byte[] serialize(final String topic, final TimestampedMetricKey data) {
//			return data.toBytes();
//		}
//	}
	
//	/**
//	 * <p>Title: TimestampedMetricKeyDeserializer</p>
//	 * <p>Description: Deserializer for TimestampedMetric instances</p> 
//	 * <p>Company: Helios Development Group LLC</p>
//	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
//	 * <p><code>com.heliosapm.streams.metrics.processors.TimestampedMetric.TimestampedMetricKeyDeserializer</code></p>
//	 */
//	public static class TimestampedMetricKeyDeserializer implements Deserializer<TimestampedMetricKey> {
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Deserializer#configure(java.util.Map, boolean)
//		 */
//		@Override
//		public void configure(final Map<String, ?> configs, final boolean isKey) {
//			/* Nop Op */			
//		}
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Deserializer#close()
//		 */
//		@Override
//		public void close() {
//			/* No Op */
//		}
//		/**
//		 * {@inheritDoc}
//		 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
//		 */
//		@Override
//		public TimestampedMetricKey deserialize(final String topic, final byte[] data) {
//			if(data==null || data.length==0) return null;
//			return new TimestampedMetricKey(data);
//		}
//	}
	
	
    private static void serialize(final long time, final long count, final byte[] into) {
    	into[0] = (byte)(time >>> 56);
    	into[1] = (byte)(time >>> 48);
    	into[2] = (byte)(time >>> 40);
    	into[3] = (byte)(time >>> 32);
    	into[4] = (byte)(time >>> 24);
    	into[5] = (byte)(time >>> 16);
    	into[6] = (byte)(time >>> 8);
    	into[7] = (byte)time;
    	into[8] = (byte)(count >>> 56);
    	into[9] = (byte)(count >>> 48);
    	into[10] = (byte)(count >>> 40);
    	into[11] = (byte)(count >>> 32);
    	into[12] = (byte)(count >>> 24);
    	into[13] = (byte)(count >>> 16);
    	into[14] = (byte)(count >>> 8);
    	into[15] = (byte)count;
    	
    }	
    
    /**
     * Deseralizes a long from the passed byte array
     * @param data The bytes to deserialize from
     * @param offset the offset in the passed byte array to start at
     * @return the long value
     */
    public static long deserialize(final byte[] data, final int offset) {
        if (data.length < 8) {
            throw new SerializationException("Size of data received by LongDeserializer is " +
                    "not >= 8");
        }
        long value = 0;
        final int offsetEnd = offset + 8;
        for(int i = offset; i < offsetEnd; i++) {
            value <<= 8;
            value |= data[i] & 0xFF;        	
        }
        return value;
    }

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((metricKey == null) ? 0 : metricKey.hashCode());
		result = prime * result + (int) (windowWidth ^ (windowWidth >>> 32));
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TimestampedMetricKey other = (TimestampedMetricKey) obj;
		if (metricKey == null) {
			if (other.metricKey != null)
				return false;
		} else if (!metricKey.equals(other.metricKey))
			return false;
		if (windowWidth != other.windowWidth)
			return false;
		return true;
	}


}
