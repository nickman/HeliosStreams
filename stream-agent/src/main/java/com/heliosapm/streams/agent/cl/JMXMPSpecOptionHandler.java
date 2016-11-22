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
package com.heliosapm.streams.agent.cl;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

/**
 * <p>Title: JMXMPSpecOptionHandler</p>
 * <p>Description: An args4j option handler for JMXMP installation specifications</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.cl.JMXMPSpecOptionHandler</code></p>
 */
public class JMXMPSpecOptionHandler extends OptionHandler<JMXMPSpec[]> {

	/**
	 * Creates a new JMXMPSpecOptionHandler
	 * @param parser The cmd line parser
	 * @param option The option definition
	 * @param setter The setter
	 */
	public JMXMPSpecOptionHandler(final CmdLineParser parser, final OptionDef option, final Setter<? super JMXMPSpec[]> setter) {
		super(parser, option, setter);
	}

	/**
	 * {@inheritDoc}
	 * @see org.kohsuke.args4j.spi.OptionHandler#getDefaultMetaVariable()
	 */
	@Override
	public String getDefaultMetaVariable() {
		return "JMXMPSpec";
	}

	/**
	 * {@inheritDoc}
	 * @see org.kohsuke.args4j.spi.OptionHandler#parseArguments(org.kohsuke.args4j.spi.Parameters)
	 */
	@Override
	public int parseArguments(final Parameters parameters) throws CmdLineException {
		final Set<JMXMPSpec> specs = new LinkedHashSet<JMXMPSpec>();
		final JMXMPSpec[] jspec = JMXMPSpec.parse(parameters.getParameter(0));
		Collections.addAll(specs, jspec);		
		if(specs.isEmpty()) specs.add(new JMXMPSpec());
		final JMXMPSpec[] jspecs = specs.toArray(new JMXMPSpec[specs.size()]);
		setter.addValue(jspecs);
		return 1;
	}

}
