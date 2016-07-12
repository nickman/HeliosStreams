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
package com.heliosapm.streams.chronicle;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * <p>Title: StreamedMetricMarshallable</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.StreamedMetricMarshallable</code></p>
 */

public class StreamedMetricMarshallable implements WriteBytesMarshallable, ReadBytesMarshallable {
	protected StreamedMetric streamedMetric = null;
	
	/**
	 * Creates a new StreamedMetricMarshallable
	 */
	public StreamedMetricMarshallable() {
		
	}

	/**
	 * Returns the streamed metric
	 * @return the streamedMetric
	 */
	public StreamedMetric getStreamedMetric() {
		return streamedMetric;
	}
	
	/**
	 * Returns the current StreamedMetric and nulls out the state
	 * @return the possibly null current StreamedMetric
	 */
	public StreamedMetric getAndNullStreamedMetric() {
		final StreamedMetric sm = streamedMetric;
		streamedMetric = null;
		return sm;
	}

	/**
	 * Sets the streamed metric
	 * @param streamedMetric the streamedMetric to set
	 * @return this StreamedMetricMarshallable
	 */
	public StreamedMetricMarshallable setStreamedMetric(final StreamedMetric streamedMetric) {
		this.streamedMetric = streamedMetric;
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.WriteBytesMarshallable#writeMarshallable(net.openhft.chronicle.bytes.BytesOut)
	 */
	@Override
	public void writeMarshallable(final BytesOut b) {
		if(streamedMetric!=null) {
			b.writeByte(StreamedMetric.ONE_BYTE);
			streamedMetric.writeMarshallable(b);
			streamedMetric = null;
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.ReadBytesMarshallable#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@Override
	public void readMarshallable(final BytesIn b) throws IORuntimeException {
		streamedMetric = StreamedMetric.fromBytes(b);
	}

}
