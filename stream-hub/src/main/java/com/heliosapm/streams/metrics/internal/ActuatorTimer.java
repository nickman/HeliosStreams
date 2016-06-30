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
package com.heliosapm.streams.metrics.internal;

import java.util.Collection;

import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.actuate.metrics.Metric;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;

/**
 * <p>Title: ActuatorTimer</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.internal.ActuatorTimer</code></p>
 */

public class ActuatorTimer extends Timer implements PublicMetrics {
	/** The system prop to override the default snapshot refresh time */
	public static final String SNAPSHOT_GAUGE_TIME = "metrics.timer.snapshot.refresh";
	/** The default snapshot refresh time in seconds */
	public static final int DEFAULT_SNAPSHOT_GAUGE_TIME = 5;
	
	/** The snapshot refresh time in seconds */
	protected final int refreshTime;
	
	/**
	 * Creates a new ActuatorTimer
	 */
	public ActuatorTimer() {
		refreshTime = getRefreshTime();
	}
	
	private static int getRefreshTime() {
		int tmp = DEFAULT_SNAPSHOT_GAUGE_TIME;
		final String c = System.getProperty(SNAPSHOT_GAUGE_TIME, "" + DEFAULT_SNAPSHOT_GAUGE_TIME);
		try {
			tmp = Integer.parseInt(c.trim());
		} catch (Exception x) {
			tmp = DEFAULT_SNAPSHOT_GAUGE_TIME;
		}
		return tmp;
	}

	/**
	 * Creates a new ActuatorTimer
	 * @param reservoir
	 */
	public ActuatorTimer(final Reservoir reservoir) {
		super(reservoir);
		refreshTime = getRefreshTime();
	}

	/**
	 * Creates a new ActuatorTimer
	 * @param reservoir
	 * @param clock
	 */
	public ActuatorTimer(final Reservoir reservoir, final Clock clock) {
		super(reservoir, clock);
		refreshTime = getRefreshTime();
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.boot.actuate.endpoint.PublicMetrics#metrics()
	 */
	@Override
	public Collection<Metric<?>> metrics() {
		// TODO Auto-generated method stub
		return null;
	}

}
