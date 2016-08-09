/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.tracing.groovy;

import com.heliosapm.streams.tracing.DefaultTracerImpl;
import com.heliosapm.streams.tracing.IMetricWriter;
import com.heliosapm.streams.tracing.ITracer;

import groovy.lang.Closure;

/**
 * <p>Title: GroovyTracer</p>
 * <p>Description: An extension of the default tracer with some special support for Groovy</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.groovy.GroovyTracer</code></p>
 */

public class GroovyTracer extends DefaultTracerImpl {

	/**
	 * Creates a new GroovyTracer
	 * @param writer the writer this tracer will write to
	 */
	public GroovyTracer(final IMetricWriter writer) {
		super(writer);
	}

	/**
	 * Pushes this tracer's state to the checkpoint stack, invokes the passed closure
	 * with this tracer as the single param and restores the tracer's state in a finally block.
	 * This guarantees that no matter what kind of mess happens in the closure, this tracer's state
	 * will always be restored once the closure returns.
	 * @param closure The closure to execute
	 * @return this tracer in the state it was before calling the closure
	 */
	public ITracer checkpoint(final Closure<Void> closure) {
		checkpoint();
		try {
			closure.call(this);
		} finally {
			popCheckpoint();
		}
		return this;
	}
	
	/**
	 * Groovy shortcut for {@link #checkpoint(Closure)}
	 * @param closure The closure to execute
	 * @return this tracer in the state it was before calling the closure
	 */
	public ITracer call(final Closure<Void> closure) {
		return checkpoint(closure); 
	}
	
	/**
	 * Pushes this tracer's state to the checkpoint stack, invokes the passed closure
	 * with this tracer as the single param and restores the tracer's state in a finally block.
	 * This guarantees that no matter what kind of mess happens in the closure, this tracer's state
	 * will always be restored once the closure returns.
	 * @param closure The closure to execute
	 * @return the elapsed time of the operation
	 */
	public long time(final Closure<Void> closure) {
		final long start = System.currentTimeMillis();
		checkpoint();
		try {
			closure.call(this);
		} catch (Exception ex) {
			log.error("Closure call error", ex);
		} finally {
			popCheckpoint();
		}
		return System.currentTimeMillis() - start;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "GroovyITracer[" + writer.toString() + "]";
	}
	
}
