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
package com.heliosapm.streams.opentsdb;

import java.util.Set;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: KafkaRPCMBean</p>
 * <p>Description: JMX MBean interface for the {@link KafkaRPC} plugin</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRPCMBean</code></p>
 */

public interface KafkaRPCMBean {
	
	/** The JMX ObjectName for this MBean */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams:service=KafkaRPC");
	
	
	/**
	 * Indicates if kafka commits are issued synchronously or not
	 * @return true if kafka commits are issued synchronously, false if they are issued after all addPoints are dispatched.
	 */
	public boolean isSyncAdd();
	
	/**
	 * Returns the total number of data points ingested
	 * @return the total number of data points ingested
	 */
	public long getTotalDataPoints();
	
	/**
	 * Returns the mean rate of incoming data points
	 * @return the mean rate of incoming data points
	 */
	public double getDataPointsMeanRate();
	
	/**
	 * Returns the last 15 minute rate of incoming data points
	 * @return the last 15 minute rate of incoming data points
	 */
	public double getDataPoints15mRate();

	/**
	 * Returns the last 5 minute rate of incoming data points
	 * @return the last 5 minute rate of incoming data points
	 */
	public double getDataPoints5mRate();

	/**
	 * Returns the last 1 minute rate of incoming data points
	 * @return the last 1 minute rate of incoming data points
	 */	
	public double getDataPoints1mRate();
	
	/**
	 * Returns the mean time to ingest a data point in ms.
	 * @return the mean time to ingest a data point in ms.
	 */
	public double getPerDataPointMeanTimeMs();
	
	/**
	 * Returns the median time to ingest a data point in ms.
	 * @return the median time to ingest a data point in ms.
	 */
	public double getPerDataPointMedianTimeMs();
	
	/**
	 * Returns the 999th percentile time to ingest a data point in ms.
	 * @return the 999th percentile time to ingest a data point in ms.
	 */
	public double getPerDataPoint999pctTimeMs();
	
	/**
	 * Returns the 99th percentile time to ingest a data point in ms.
	 * @return the 99th percentile time to ingest a data point in ms.
	 */
	public double getPerDataPoint99pctTimeMs();

	/**
	 * Returns the 75th percentile time to ingest a data point in ms.
	 * @return the 75th percentile time to ingest a data point in ms.
	 */
	public double getPerDataPoint75pctTimeMs();
	
	/**
	 * Returns the mean rate of incoming data point batches
	 * @return the mean rate of incoming data point batches
	 */
	public double getBatchMeanRate();
	
	/**
	 * Returns the last 15 minute rate of incoming data point batches
	 * @return the last 15 minute rate of incoming data point batches
	 */
	public double getBatch15mRate();

	/**
	 * Returns the last 5 minute rate of incoming data point batches
	 * @return the last 5 minute rate of incoming data point batches
	 */
	public double getBatch5mRate();

	/**
	 * Returns the last 1 minute rate of incoming data point batches
	 * @return the last 1 minute rate of incoming data point batches
	 */
	public double getBatch1mRate();
	
	/**
	 * Returns the number of data points pending write confirmation 
	 * @return the number of data points pending write confirmation
	 */
	public long getPendingDataPointAdds();
	
	/**
	 * Returns the currently assigned topic partitions
	 * @return the currently assigned topic partitions
	 */
	public Set<String> getAssignedPartitions();

}
