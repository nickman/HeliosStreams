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
package com.heliosapm.streams.agent.util;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * <p>Title: SimpleLogger</p>
 * <p>Description: Low maintenance logger</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.util.SimpleLogger</code></p>
 */

public class SimpleLogger {

	
	/** Agent log file */
	private static volatile File agentLogFile = null;

	/**
	 * Low maintenance formatted out logger
	 * @param fmt The message format
	 * @param args The message fillins
	 */
	public static void log(final Object fmt, final Object...args) {
		if(agentLogFile!=null) {
			log(agentLogFile, fmt, args);
		} else {
			System.out.println(String.format(fmt.toString(), args));
		}
	}
	
	/**
	 * Low maintenance formatted file logger.
	 * Not very efficient, but not expected to be called much.
	 * @param fmt The message format
	 * @param args The message fillins
	 */
	public static void log(final File outFile, final Object fmt, final Object...args) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(outFile);
			fw.write(String.format(fmt.toString(), args) + "\n");
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write to file [" + outFile + "]", ex);
		} finally {
			if(fw!=null) {
				try { fw.flush(); } catch(Exception x) {} 
				try { fw.close(); } catch(Exception x) {}
			}
		}
		System.out.println(String.format(fmt.toString(), args));
	}
	
	
	/**
	 * Low maintenance formatted err logger
	 * @param fmt The message format
	 * @param t An optional throwable
	 * @param args The message fillins
	 */
	public static void elog(final Object fmt, final Throwable t, final Object...args) {
		if(agentLogFile!=null) {
			log(agentLogFile, "ERROR:" + fmt, args);
			PrintWriter pw = null;
			if(t!=null) {
				try {
					pw = new PrintWriter(agentLogFile);
					t.printStackTrace(pw);
				} catch (Exception ex) {
					throw new RuntimeException("Failed to write stack trace to agent log file", ex);
				} finally {
					if(pw!=null) {
						try { pw.flush(); } catch (Exception x) {/* No Op */}
						try { pw.close(); } catch (Exception x) {/* No Op */}
					}
				}				
			}
		} else {
			System.err.println(String.format(fmt.toString(), args));
			if(t!=null) t.printStackTrace(System.err);
		}
	}

	/**
	 * Low maintenance formatted err logger
	 * @param fmt The message format
	 * @param args The message fillins
	 */
	public static void elog(final Object fmt, final Object...args) {
		elog(fmt, null, args);
	}

	/**
	 * Returns the agent's log file
	 * @return the agent's log file
	 */
	public static File getAgentLogFile() {
		return agentLogFile;
	}

	/**
	 * Sets the agent's log file
	 * @param agentLogFile the agent's log file
	 */
	public static void setAgentLogFile(final File agentLogFile) {
		SimpleLogger.agentLogFile = agentLogFile;
	}

}
