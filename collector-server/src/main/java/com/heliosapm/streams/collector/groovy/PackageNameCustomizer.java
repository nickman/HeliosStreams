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
package com.heliosapm.streams.collector.groovy;

import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

/**
 * <p>Title: PackageNameCustomizer</p>
 * <p>Description: Forces the intended class and package name</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.PackageNameCustomizer</code></p>
 */

public class PackageNameCustomizer extends CompilationCustomizer {

	/**
	 * Creates a new PackageNameCustomizer
	 */
	public PackageNameCustomizer() {
		super(CompilePhase.CONVERSION);
	}

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.CompilationUnit.PrimaryClassNodeOperation#call(org.codehaus.groovy.control.SourceUnit, org.codehaus.groovy.classgen.GeneratorContext, org.codehaus.groovy.ast.ClassNode)
	 */
	@Override
	public void call(final SourceUnit source, final GeneratorContext context, final ClassNode classNode) throws CompilationFailedException {
		classNode.setName(source.getName());
		final Class<?>[] ifaces = ManagedScript.class.getInterfaces();
		for(Class<?> iface: ifaces) {
			classNode.addInterface(new ClassNode(iface));
		}
	}

}
