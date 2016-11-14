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
package com.heliosapm.streams.collector.groovy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.ast.AnnotationNode;
import org.codehaus.groovy.ast.ClassHelper;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.ImportNode;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.transform.GroovyASTTransformation;

/**
 * <p>Title: GlobalAnnotationsSuppressor</p>
 * <p>Description: Customizer to suppress unwanted global annotations</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.GlobalAnnotationsSuppressor</code></p>
 */

public class GlobalAnnotationsSuppressor extends CompilationCustomizer {
	/** Global annotations to suppress */
	private static final Set<String> SUPRESS_GLOBALS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"groovy.lang.Grab", "groovy.lang.Grapes"
	)));
	/** Instance logger */	
	private final Logger log = LogManager.getLogger(getClass());

	/**
	 * Creates a new GlobalAnnotationsSuppressor
	 * @param phase
	 */
	public GlobalAnnotationsSuppressor() {
		super(CompilePhase.CANONICALIZATION);
	}

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.CompilationUnit.PrimaryClassNodeOperation#call(org.codehaus.groovy.control.SourceUnit, org.codehaus.groovy.classgen.GeneratorContext, org.codehaus.groovy.ast.ClassNode)
	 */
	@Override
	public void call(SourceUnit source, GeneratorContext context, ClassNode classNode) throws CompilationFailedException {
		// Disable global transformations
		classNode.addAnnotation(new AnnotationNode(ClassHelper.make(GroovyASTTransformation.class)));
		for(ImportNode im: source.getAST().getImports()) {
			for(AnnotationNode an: im.getAnnotations()) {
				log.info("AnnotationNode: [{}]", an.getClassNode().getName());
			}
		}


	}

}
