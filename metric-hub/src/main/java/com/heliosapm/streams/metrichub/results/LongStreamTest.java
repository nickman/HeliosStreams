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
package com.heliosapm.streams.metrichub.results;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

/**
 * <p>Title: LongStreamTest</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.results.LongStreamTest</code></p>
 */

public class LongStreamTest {

	/**
	 * Creates a new LongStreamTest
	 */
	public LongStreamTest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log("ParallelLongBuilder Test");
		int SZ = 1000;
		final Set<long[]> dps = Collections.synchronizedSortedSet(new TreeSet<long[]>(QueryResult.DPS_COMPARATOR));		
		IntStream.range(0, SZ-1).parallel().forEach(i -> dps.add(new long[]{i, i*i}));
		log("Set Prepared");
		double d1 = 0, d2 = 0;
		for(int i = 0; i < 20000; i++) {
			d1 += dpsStream(dps).sum();
			d2 += dpsStream(dps).average().getAsDouble();
		}
		SZ = 10000000;
		final Set<long[]> dps2 = Collections.synchronizedSortedSet(new TreeSet<long[]>(QueryResult.DPS_COMPARATOR));		
		IntStream.range(0, SZ-1).parallel().forEach(i -> dps2.add(new long[]{i, i*i}));
		log("Count:" + dpsStream(dps2).count());
		ElapsedTime et = SystemClock.startClock();
		log("Sum:" + dpsStream(dps2).sum());
		log(et.printAvg("Sum", SZ));
		et = SystemClock.startClock();		
		log("Avg:" + dpsStream(dps2).average().getAsDouble());
		log(et.printAvg("Avg", SZ));

	}
	
	private static long time(final long[] dp) { return dp[0]; }
	
	protected static LongStream dpsStream(final Set<long[]> dps) {
		return StreamSupport.stream(dps.spliterator(), true).flatMapToLong(arr -> LongStream.of(arr[0])).parallel();
	}
	
	
	public static void log(final Object fmt, final Object...args) {
		System.out.println(String.format(fmt.toString(), args));
	}

}
