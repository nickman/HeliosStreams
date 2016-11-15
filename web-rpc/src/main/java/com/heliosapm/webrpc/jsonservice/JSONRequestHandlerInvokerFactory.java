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
package com.heliosapm.webrpc.jsonservice;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.webrpc.annotations.JSONRequestHandler;
import com.heliosapm.webrpc.annotations.JSONRequestService;
import com.heliosapm.webrpc.jsonservice.netty3.Netty3JSONRequest;

import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.Modifier;


/**
 * <p>Title: JSONRequestHandlerInvokerFactory</p>
 * <p>Description: A factory for generating json request handler invokers.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.jsonservice.JSONRequestHandlerInvokerFactory</code></p>
 */

public class JSONRequestHandlerInvokerFactory {
	/** Static class logger */
	protected static final Logger LOG = LogManager.getLogger(JSONRequestHandlerInvokerFactory.class);
	/** Cache of created invoker maps keyed by target class */
	protected static final Map<Class<?>, Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>> invokerCache = new ConcurrentHashMap<Class<?>, Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>>();

	/**
	 * Creates a map of concrete json request handler invokers keyed by <b><code>&lt;service-name&gt;/&lt;op-name&gt;</code></b>.
	 * @param handlerInstance The request handler instance to generate invokers for
	 * @return the map of generated invokers
	 */
	public static Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> createInvokers(Object handlerInstance) {
		if(handlerInstance==null) throw new IllegalArgumentException("The passed handlerInstance was null");
		Map<String, AbstractJSONRequestHandlerInvoker> subInvokerMap = new HashMap<String, AbstractJSONRequestHandlerInvoker>();
		Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> invokerMap = invokerCache.get(handlerInstance.getClass());
		if(invokerMap!=null) {
			LOG.info("Found Cached Invokers for [{}]", handlerInstance.getClass().getName());
			return invokerMap;
		}
		invokerMap = new HashMap<String, Map<String, AbstractJSONRequestHandlerInvoker>>(1);
		
		LOG.info("Generating Invokers for [{}]", handlerInstance.getClass().getName());
		JSONRequestService svc = handlerInstance.getClass().getAnnotation(JSONRequestService.class);
		final String invokerServiceKey = svc.name();
		final String invokerServiceDescription = svc.description();
		
		
		invokerMap.put(invokerServiceKey, subInvokerMap);
		
		ClassPool cp = new ClassPool();
		cp.appendClassPath(new ClassClassPath(handlerInstance.getClass()));
		cp.appendClassPath(new ClassClassPath(AbstractJSONRequestHandlerInvoker.class));
		cp.importPackage(handlerInstance.getClass().getPackage().getName());
		Set<ClassLoader> classPathsAdded = new HashSet<ClassLoader>();
		Set<String> packagesImported = new HashSet<String>(); 
		try {
			final CtClass jsonRequestCtClass = cp.get(JSONRequest.class.getName());
			final CtClass netty3JsonRequestCtClass = cp.get(Netty3JSONRequest.class.getName());
			final CtClass parent = cp.get(AbstractJSONRequestHandlerInvoker.class.getName());
			CtClass targetClass = cp.get(handlerInstance.getClass().getName());
			Collection<Method> methods = getTargetMethods(handlerInstance.getClass());
			for(Method m: methods) {
				final JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
				final String opName = jsonHandler.name();
				final String opDescription = jsonHandler.description();
				final RequestType opType = jsonHandler.type();
				

				
				int targetMethodHashCode = m.toGenericString().hashCode(); 
				final String className = String.format("%s-%s%s-%s-%s", 
						handlerInstance.getClass().getName(), invokerServiceKey, opName, "ServiceInvoker", targetMethodHashCode);
				
				final CtClass invokerClass = cp.makeClass(className, parent);
				CtField ctf = new CtField(targetClass, "typedTarget", invokerClass);
				ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
				invokerClass.addField(ctf);
				for(CtConstructor parentCtor: parent.getConstructors()) {
					CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
					invokerCtor.setBody("{ super($$); typedTarget = (" + handlerInstance.getClass().getName() + ")$1; }");
					invokerClass.addConstructor(invokerCtor);					
				}
				final StringBuilder b = new StringBuilder();
				final CtMethod invokerMethod;
				if(m.getParameterTypes()[0].getName().equals(JSONRequest.class.getName())) {
					invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {jsonRequestCtClass}), invokerClass, null);
				} else {
					invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {netty3JsonRequestCtClass}), invokerClass, null);
				}
				b.append("{this.typedTarget.")
					.append(m.getName())
					.append("($1");
				final Class<?>[] ptypes = m.getParameterTypes();
				final int remainingParamCount = ptypes.length-1;
//				Set<Class<?>> classPathsAdded = new HashSet<Class<?>>();
//				Set<String> packagesImported = new HashSet<String>(); 				
				if(remainingParamCount>0) {
					for(int i = 0; i < remainingParamCount; i++) {						
						final Class<?> type = ptypes[i+1];
						if(type.getName().contains("UniqueIdType")) {
							System.err.println("Comin Up....");
						}
						if(type.isPrimitive()) {
							b.append(", (").append(type.getName()).append(") null");
						} else {
							if(classPathsAdded.add(type.getClassLoader())) {
								cp.appendClassPath(new LoaderClassPath(type.getClassLoader()));
							}
							try {								
								Package p = type.getPackage();
								if(p==null) {
									if(type.isArray()) {
										if(!type.getComponentType().isPrimitive()) {
											p = type.getComponentType().getPackage();
										}
									}
								}
								if(type.isEnum()) {
									final String f = type.getEnclosingClass().getName() + "." + type.getSimpleName();
									b.append(", (").append(f).append(") null");
									String pack = type.getEnclosingClass().getPackage().getName();
									if(packagesImported.add(pack)) {
										cp.importPackage(pack);	
									}									
									continue;
								}

								
								if(p!=null) {									
									if(packagesImported.add(p.getName())) {
										cp.importPackage(p.getName());
									}
								}
							} catch (Exception ex) {
								ex.printStackTrace(System.err);
							}
							b.append(", (").append(type.getSimpleName()).append(") null");
						}						
					}
				}
				
				b.append(");}");
				System.out.println("[" + m.getName() + "]: [" + b.toString() + "]");
				//invokerMethod.setBody("{this.typedTarget." + m.getName() + "($1);}");
				invokerMethod.setBody(b.toString());
				invokerMethod.setModifiers(invokerMethod.getModifiers() & ~Modifier.ABSTRACT);
				invokerClass.addMethod(invokerMethod);
				//invokerClass.writeFile(System.getProperty("java.io.tmpdir") + File.separator + "jsoninvokers");
				Class<?> clazz = invokerClass.toClass(handlerInstance.getClass().getClassLoader(), handlerInstance.getClass().getProtectionDomain());
				Constructor<?> ctor = clazz.getDeclaredConstructor(Object.class, String.class, String.class, String.class, String.class, RequestType.class);
				AbstractJSONRequestHandlerInvoker invokerInstance = (AbstractJSONRequestHandlerInvoker)ctor.newInstance(handlerInstance, invokerServiceKey, invokerServiceDescription, opName, opDescription, opType);
				subInvokerMap.put(opName, invokerInstance);				
			}
			invokerCache.put(handlerInstance.getClass(), invokerMap);
			return invokerMap;
		} catch (Exception ex) {
			LOG.error("Failed to create RequestHandlerInvoker for [{}]", handlerInstance.getClass().getName(), ex);
			throw new RuntimeException("Failed to create RequestHandlerInvoker [" + handlerInstance.getClass().getName() + "]", ex);
		}
		
	}
	
	
	public static void main(String[] args) {
		createInvokers(new FooService());
		createInvokers(new FooService());
	}
	
	@JSONRequestService(name="foo")
	public static class FooService {
		@JSONRequestHandler(name="bar1")
		public void bar1(JSONRequest request, String a) {
			
		}
		@JSONRequestHandler(name="bar0")
		public void bar0(JSONRequest request) {
			
		}
		@JSONRequestHandler(name="bar3")
		public void bar3(JSONRequest request, String a, Integer x, Long...longs) {
			
		}
		
	}
	
	private static boolean containsPrimitives(Class<?>[] ptypes) {
		if(ptypes==null || ptypes.length==0) return false;
		for(Class<?> c: ptypes) {
			if(c.isPrimitive()) return true;
		}
		return false;
	}
	
	/**
	 * Finds and returns the valid target {@link JSONRequestHandler} annotated methods in the passed class.
	 * @param clazz the class to inspect
	 * @return a collection of valid json request methods
	 */
	public static Collection<Method> getTargetMethods(Class<?> clazz) {
		Map<String, Method> mappedMethods = new HashMap<String, Method>();
		for(Method m: clazz.getMethods()) {
			JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
			if(jsonHandler!=null) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if(paramTypes.length<1 || (!JSONRequest.class.equals(paramTypes[0]) && !Netty3JSONRequest.class.equals(paramTypes[0])) || containsPrimitives(paramTypes)) {
					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
					continue;
				}
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
//				Class<?>[] paramTypes = m.getParameterTypes();
//				if(paramTypes.length!=1 || !JSONRequest.class.equals(paramTypes[0])) {
//					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
//					continue;
//				}
//				mappedMethods.put(m.getName(), m);
			}
		}
		for(Method m: clazz.getDeclaredMethods()) {
			JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
			if(jsonHandler!=null) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if(paramTypes.length<1 || (!JSONRequest.class.equals(paramTypes[0]) && !Netty3JSONRequest.class.equals(paramTypes[0]))) {
					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
					continue;
				}
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
//				Class<?>[] paramTypes = m.getParameterTypes();
//				if(paramTypes.length!=1 || !JSONRequest.class.equals(paramTypes[0])) {
//					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
//					continue;
//				}
//				mappedMethods.put(m.getName(), m);
			}			
		}
		return mappedMethods.values();
		
	}
	
	/**
	 * Finds and returns the valid target {@link JSONRequestHandler} annotated methods in the passed class.
	 * @param clazz the class to inspect
	 * @return a collection of valid json request methods
	 */
	public static Collection<Method[]> getTargetMethodPairs(Class<?> clazz) {
		Map<String, Method> mappedMethods = new HashMap<String, Method>();
		for(Method m: clazz.getMethods()) {
			if(m.getAnnotation(JSONRequestHandler.class)!=null) {
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
			}
		}
		for(Method m: clazz.getDeclaredMethods()) {
			if(m.getAnnotation(JSONRequestHandler.class)!=null) {
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
			}
		}
		
		for(Method m: mappedMethods.values()) {
			final Method[] pair = new Method[2];
			JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
			if(jsonHandler!=null) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if(paramTypes.length<1 || (!JSONRequest.class.equals(paramTypes[0]) && !Netty3JSONRequest.class.equals(paramTypes[0])) || containsPrimitives(paramTypes)) {
					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
					continue;
				}
				pair[JSONRequest.class.equals(paramTypes[0]) ? 0 : 1]  = m;
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
//				Class<?>[] paramTypes = m.getParameterTypes();
//				if(paramTypes.length!=1 || !JSONRequest.class.equals(paramTypes[0])) {
//					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
//					continue;
//				}
//				mappedMethods.put(m.getName(), m);
			}
		}
		for(Method m: clazz.getDeclaredMethods()) {
			JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
			if(jsonHandler!=null) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if(paramTypes.length<1 || (!JSONRequest.class.equals(paramTypes[0]) && !Netty3JSONRequest.class.equals(paramTypes[0]))) {
					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
					continue;
				}
				mappedMethods.put(m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")", m);
//				Class<?>[] paramTypes = m.getParameterTypes();
//				if(paramTypes.length!=1 || !JSONRequest.class.equals(paramTypes[0])) {
//					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
//					continue;
//				}
//				mappedMethods.put(m.getName(), m);
			}			
		}
		return null; //mappedMethods.values();
		
	}
	
	
	private JSONRequestHandlerInvokerFactory() {
	}

}
