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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.webrpc.annotations.JSONRequestHandler;
import com.heliosapm.webrpc.annotations.JSONRequestService;
import com.heliosapm.webrpc.jsonservice.AbstractJSONRequestHandlerInvoker.NettyTypeDescriptor;
import com.heliosapm.webrpc.jsonservice.netty3.Netty3JSONRequest;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.Modifier;
import javassist.NotFoundException;
import net.openhft.hashing.LongHashFunction;


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
	protected static final Cache<Class<?>, Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>> invokerCache = 
			CacheBuilder.newBuilder().concurrencyLevel(Runtime.getRuntime().availableProcessors())
			.initialCapacity(1024)
			.weakKeys()
			.build();
	//protected static final Map<Class<?>, Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>> invokerCache = new ConcurrentHashMap<Class<?>, Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>>();

	/** The netty 4 JSONRequest CtClass */
	private static final CtClass jsonRequestCtClass;
	/** The netty 3 JSONRequest CtClass */
	private static final CtClass netty3JsonRequestCtClass;
	/** The abstract json service invoker CtClass */
	private static final CtClass parent;
	/** The JSONRequest ct classes as an array */
	private static final CtClass[] jsonRequestCtClasses;
	/** The JSONRequest java classes as an array */
	private static final Class<?>[] jsonRequestClasses = new Class[]{JSONRequest.class, Netty3JSONRequest.class};
	/** The JSONRequest type names */
	private static final String[] jsonRequestTypeNames = {"Netty4", "Netty3"};
	/** The JSONRequest java classes */
	public static final Set<Class<?>> JSONREQUEST_CLASSES = Collections.unmodifiableSet(new HashSet<Class<?>>(Arrays.asList(jsonRequestClasses)));
	/** The jsonrequest class indexes */
	public static final Map<Class<?>, Integer> jsonRequestIndexes;
	/** The name of the json request type keyed by the index */
	public static final Map<Integer, String> jsonRequestTypeNamesByIndex;
	
	/** Maps the java jsonRequest class to the ct class */
	public static final Map<Class<?>, CtClass> javaToCtMap;
	
	/** Zeor alloc hasher */
	private static final LongHashFunction hasher = LongHashFunction.murmur_3();
	
	/** The number of jsonrequest classes supported */
	public static final int NETTY_VERSIONS; 
	
	
	static {
		final ClassPool cp; 
		try {
			cp = new ClassPool();			
			cp.appendClassPath(new ClassClassPath(AbstractJSONRequestHandlerInvoker.class));
			jsonRequestCtClass = cp.get(JSONRequest.class.getName());
			netty3JsonRequestCtClass = cp.get(Netty3JSONRequest.class.getName());
			parent = cp.get(AbstractJSONRequestHandlerInvoker.class.getName());
			jsonRequestCtClasses = new CtClass[]{jsonRequestCtClass, netty3JsonRequestCtClass};
			NETTY_VERSIONS = jsonRequestClasses.length;
			Map<Class<?>, Integer> tmp = new HashMap<Class<?>, Integer>(NETTY_VERSIONS);
			Map<Integer, String> tmpNames = new HashMap<Integer, String>(NETTY_VERSIONS);
			Map<Class<?>, CtClass> tmpMapping = new HashMap<Class<?>, CtClass>(NETTY_VERSIONS);
			tmpMapping.put(JSONRequest.class, jsonRequestCtClass);
			tmpMapping.put(Netty3JSONRequest.class, netty3JsonRequestCtClass);
			for(int i = 0; i < NETTY_VERSIONS; i++) {
				tmp.put(jsonRequestClasses[i], i);
				tmpNames.put(i, jsonRequestTypeNames[i]);
			}
			jsonRequestIndexes = Collections.unmodifiableMap(tmp);
			jsonRequestTypeNamesByIndex = Collections.unmodifiableMap(tmpNames);
			javaToCtMap = Collections.unmodifiableMap(tmpMapping);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to initialize JSONRequestHandlerInvokerFactory", ex);
		}
	}

	private static final Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> EMPTY_INVOKER_MAP = 
		Collections.unmodifiableMap(Collections.emptyMap());

	
	
	private static Map<String, Method[]> mapByOp(final Map<Class<?>, Map<String, Method>> targetMethods) {
		final Map<String, Method[]> remappedMethods = new HashMap<String, Method[]>();
		for(final Map<String, Method> methods: targetMethods.values()) {
			for(final Method method : methods.values()) {
				final Class<?> jsonRequestClass = method.getParameterTypes()[0];
				final JSONRequestHandler jsonHandler = method.getAnnotation(JSONRequestHandler.class);
				final String op = jsonHandler.name();
				Method[] methodPair = remappedMethods.get(op);
				if(methodPair==null) {
					methodPair = new Method[NETTY_VERSIONS];
					remappedMethods.put(op, methodPair);
				}
				final int index = jsonRequestIndexes.get(jsonRequestClass);
				methodPair[index] = method;
			}
		}
		
		// -->   <OpName, <MethodName, Method>>
		return remappedMethods;
	}
	
	private static AbstractJSONRequestHandlerInvoker generate(final Object handlerInstance, final String opName, final Method[] methods) {
		final Class<?> handlerType = handlerInstance.getClass();
		try {
			final ClassPool cp = new ClassPool();
			cp.appendClassPath(new ClassClassPath(handlerType));
			cp.appendClassPath(new ClassClassPath(AbstractJSONRequestHandlerInvoker.class));
			cp.importPackage(handlerType.getPackage().getName());
//			public abstract void doInvoke(final JSONRequest jsonRequest);
//			public abstract void doInvoke(final Netty3JSONRequest jsonRequest);
			final JSONRequestService jrs = handlerType.getAnnotation(JSONRequestService.class); 
			final String invokerServiceKey = jrs.name();
			final String ctClassName = String.format("%s-%s%s-%s-%s", 
					handlerType.getName(), invokerServiceKey, opName, "ServiceInvoker", hasher.hashChars(new StringBuilder(handlerType.getName())
						.append(opName).append(invokerServiceKey))
			);
			final CtClass handlerTypeCtClass = cp.get(handlerType.getName());
			final CtClass invokerClass = cp.makeClass(ctClassName, parent);
			// ==== the field that holds the handler instance
			CtField ctf = new CtField(handlerTypeCtClass, "typedTarget", invokerClass);
			ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
			invokerClass.addField(ctf);
			// ==== the subclass ctor
			for(CtConstructor parentCtor: parent.getConstructors()) {
				CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
				invokerCtor.setBody("{ super($$); typedTarget = (" + handlerType.getName() + ")$1; }");
				invokerClass.addConstructor(invokerCtor);					
			}
			// ==== implement the doInvoke methods
			for(final Map.Entry<Class<?>, Integer> entry: jsonRequestIndexes.entrySet()) {
				final int index = entry.getValue();
				final Class<?> jsonRequestType = entry.getKey();
				final Method gm = methods[index];
				if(gm==null) {
					generateNoOpCtMethod(jsonRequestType, invokerClass);
				} else {
					generateCtMethod(jsonRequestType, gm, invokerClass);
				}
			}
			// ==== build the class and instantiate
			final Class<?> clazz = invokerClass.toClass(handlerType.getClassLoader(), handlerType.getProtectionDomain());
//			public AbstractJSONRequestHandlerInvoker(
//					final Object targetService, final String serviceName, 
//					final String serviceDescription, final String opName, 
//					final RequestType type, final Map<Class<?>, NettyTypeDescriptor> nettyDescriptors) 
			final Constructor<?> ctor = clazz.getDeclaredConstructor(Object.class, String.class, String.class, String.class, RequestType.class, Map.class);
			AbstractJSONRequestHandlerInvoker invokerInstance = (AbstractJSONRequestHandlerInvoker)ctor.newInstance(handlerInstance, invokerServiceKey, opName, opType);
			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to generate invoker for type [" + handlerType.getSimpleName() + "], op: [" + opName + "]", ex);
		}			
	}
	
	private static void generateNoOpCtMethod(final Class<?> jsonRequestType, final CtClass invokerClass) throws CannotCompileException, NotFoundException {
		final CtMethod noOpMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}), parent, null); 
				//new CtMethod(CtClass.voidType, "doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}, invokerClass);
		noOpMethod.setBody("{throw new " + UnsupportedOperationException.class.getName() + "();}");
		noOpMethod.setModifiers(noOpMethod.getModifiers() & ~Modifier.ABSTRACT);
		invokerClass.addMethod(noOpMethod);
	}
	
	private static void generateCtMethod(final Class<?> jsonRequestType, final Method m, final CtClass invokerClass) throws CannotCompileException, NotFoundException {
		final CtMethod opMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}), parent, null);
		//final CtMethod opMethod = new CtMethod(CtClass.voidType, "doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}, invokerClass);
		opMethod.setBody("{this.typedTarget." + m.getName() + "($1);}");
		opMethod.setModifiers(opMethod.getModifiers() & ~Modifier.ABSTRACT);
		invokerClass.addMethod(opMethod);
	}
	
	/**
	 * Creates a map of concrete json request handler invokers keyed by <b><code>&lt;service-name&gt;/&lt;op-name&gt;</code></b>.
	 * @param handlerInstance The request handler instance to generate invokers for
	 * @return the map of generated invokers
	 */
	public static Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> createInvokers(final Object handlerInstance) {
		if(handlerInstance==null) throw new IllegalArgumentException("The passed handlerInstance was null");
		final Class<?> clazz = handlerInstance.getClass();
		final JSONRequestService requestService = clazz.getAnnotation(JSONRequestService.class);
		
		return invokerCache.get(clazz, new Callable<Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>>(){
			@Override
			public Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> call() throws Exception {
				final Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> invokerMap = new ConcurrentHashMap<String, Map<String, AbstractJSONRequestHandlerInvoker>>();
				final Map<Class<?>, Map<String, Method>> targetMethods = getTargetMethods(clazz);
				if(targetMethods.isEmpty()) return EMPTY_INVOKER_MAP;
				final Map<String, Method[]> methodPairsByOp = mapByOp(targetMethods);
				
				return invokerMap;
			}
		});
		
		
		final Map<String, AbstractJSONRequestHandlerInvoker> subInvokerMap = new HashMap<String, AbstractJSONRequestHandlerInvoker>();
		
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
			final CtClass targetClass = cp.get(handlerInstance.getClass().getName());
			
			final Collection<Method[]> methods = getTargetMethodPairs(handlerInstance.getClass());
			for(final Method[] methodPair: methods) {
				final boolean hasBoth = methodPair[0] != null && methodPair[1] !=null; 
				final boolean hasNeither = methodPair[0] == null && methodPair[1] == null;
				if(hasNeither) continue;
				
				for(int i = 0; i < 2; i++) {
					final Method m = methodPair[i];
					final Method otherm = methodPair[i==0 ? 1 : 0];
					final JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
					final StringBuilder b = new StringBuilder();
					final String opName = jsonHandler.name();
					final String opDescription = jsonHandler.description() + " (Netty" + (i==0 ? "4" : "3") + ")";
					final RequestType opType = jsonHandler.type();					
					final CtMethod invokerMethod;
					final String className;
					final CtClass invokerClass;
					if(m!=null) {
						
						int targetMethodHashCode = m.toGenericString().hashCode(); 
						className = String.format("%s-%s%s-%s-%s", 
								handlerInstance.getClass().getName(), invokerServiceKey, opName, "ServiceInvoker", targetMethodHashCode);
						invokerClass = cp.makeClass(className, parent);
						CtField ctf = new CtField(targetClass, "typedTarget", invokerClass);
						ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
						invokerClass.addField(ctf);
						for(CtConstructor parentCtor: parent.getConstructors()) {
							CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
							invokerCtor.setBody("{ super($$); typedTarget = (" + handlerInstance.getClass().getName() + ")$1; }");
							invokerClass.addConstructor(invokerCtor);					
						}
						invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {(i==0 ? jsonRequestCtClass : netty3JsonRequestCtClass)}), invokerClass, null);
						b.append("{this.typedTarget.")
							.append(m.getName())
							.append("($1");
						b.append(");}");
					} else {
						int targetMethodHashCode = m.toGenericString().hashCode();
						className = String.format("%s-%s%s-%s-%s", 
								handlerInstance.getClass().getName(), invokerServiceKey, opName, "ServiceInvoker", targetMethodHashCode);
						invokerClass = cp.makeClass(className, parent);
						CtField ctf = new CtField(targetClass, "typedTarget", invokerClass);
						ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
						invokerClass.addField(ctf);
						for(CtConstructor parentCtor: parent.getConstructors()) {
							CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
							invokerCtor.setBody("{ super($$); typedTarget = (" + handlerInstance.getClass().getName() + ")$1; }");
							invokerClass.addConstructor(invokerCtor);					
						}
						invokerMethod.setModifiers(invokerMethod.getModifiers() & ~Modifier.ABSTRACT);
						invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {(i==0 ? jsonRequestCtClass : netty3JsonRequestCtClass)}), invokerClass, null);
						b.append("{this.typedTarget.")
							.append(m.getName())
							.append("($1");
						b.append(");}");
						
					}
					LOG.info("[" + m.getName() + "]: [" + b.toString() + "]");
					invokerMethod.setBody(b.toString());
					invokerMethod.setModifiers(invokerMethod.getModifiers() & ~Modifier.ABSTRACT);
					invokerClass.addMethod(invokerMethod);
					//invokerClass.writeFile(System.getProperty("java.io.tmpdir") + File.separator + "jsoninvokers");
					Class<?> clazz = invokerClass.toClass(handlerInstance.getClass().getClassLoader(), handlerInstance.getClass().getProtectionDomain());
					Constructor<?> ctor = clazz.getDeclaredConstructor(Object.class, String.class, String.class, String.class, String.class, RequestType.class);
					AbstractJSONRequestHandlerInvoker invokerInstance = (AbstractJSONRequestHandlerInvoker)ctor.newInstance(handlerInstance, invokerServiceKey, invokerServiceDescription, opName, opDescription, opType);
					subInvokerMap.put(opName, invokerInstance);										
				}
			}
			invokerCache.put(handlerInstance.getClass(), invokerMap);
			return invokerMap;
		} catch (Exception ex) {
			LOG.error("Failed to create RequestHandlerInvoker for [{}]", handlerInstance.getClass().getName(), ex);
			throw new RuntimeException("Failed to create RequestHandlerInvoker [" + handlerInstance.getClass().getName() + "]", ex);
		}		
	}
	
	/** The number of JSONRequest versions implemented */
	public static final int VERSIONS = 2;
	
	private static CtClass[]  processMethodPair(final Method[] methodPair, final Class<?> handlerClass) {
		if(methodPair==null) throw new IllegalArgumentException("Method pair was null. PROGRAMMER ERROR.");
		if(methodPair.length!=VERSIONS) throw new IllegalArgumentException("Method pair length invalid. Was [" + methodPair.length + "] but expected [" + VERSIONS + "]. PROGRAMMER ERROR.");
		final CtClass[] ct = new CtClass[VERSIONS];
		final int nullMask = Arrays.stream(methodPair)
			.mapToInt(m -> m==null ? 0 : 1)
			.sum();
		
		if(nullMask==0) return ct;
		final boolean hasAll = nullMask==methodPair.length;
		
		
		
		final Method m = methodPair[i];
		final Method otherm = methodPair[i==0 ? 1 : 0];
		final JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
		final StringBuilder b = new StringBuilder();
		final String opName = jsonHandler.name();
		final String opDescription = jsonHandler.description() + " (Netty" + (i==0 ? "4" : "3") + ")";
		final RequestType opType = jsonHandler.type();					
		final CtMethod invokerMethod;
		final String className;
		final CtClass invokerClass;
		if(m!=null) {
			
			int targetMethodHashCode = m.toGenericString().hashCode(); 
			className = String.format("%s-%s%s-%s-%s", 
					handlerClass.getName(), invokerServiceKey, opName, "ServiceInvoker", targetMethodHashCode);
			invokerClass = cp.makeClass(className, parent);
			CtField ctf = new CtField(targetClass, "typedTarget", invokerClass);
			ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
			invokerClass.addField(ctf);
			for(CtConstructor parentCtor: parent.getConstructors()) {
				CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
				invokerCtor.setBody("{ super($$); typedTarget = (" + handlerInstance.getClass().getName() + ")$1; }");
				invokerClass.addConstructor(invokerCtor);					
			}
			invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {(i==0 ? jsonRequestCtClass : netty3JsonRequestCtClass)}), invokerClass, null);
			b.append("{this.typedTarget.")
				.append(m.getName())
				.append("($1");
			b.append(");}");
		} else {
			int targetMethodHashCode = m.toGenericString().hashCode();
			className = String.format("%s-%s%s-%s-%s", 
					handlerInstance.getClass().getName(), invokerServiceKey, opName, "ServiceInvoker", targetMethodHashCode);
			invokerClass = cp.makeClass(className, parent);
			CtField ctf = new CtField(targetClass, "typedTarget", invokerClass);
			ctf.setModifiers(ctf.getModifiers() | Modifier.FINAL);
			invokerClass.addField(ctf);
			for(CtConstructor parentCtor: parent.getConstructors()) {
				CtConstructor invokerCtor = CtNewConstructor.copy(parentCtor, invokerClass, null);
				invokerCtor.setBody("{ super($$); typedTarget = (" + handlerInstance.getClass().getName() + ")$1; }");
				invokerClass.addConstructor(invokerCtor);					
			}
			invokerMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[] {(i==0 ? jsonRequestCtClass : netty3JsonRequestCtClass)}), invokerClass, null);
			b.append("{this.typedTarget.")
				.append(m.getName())
				.append("($1");
			b.append(");}");
			
		}
		LOG.info("[" + m.getName() + "]: [" + b.toString() + "]");
		invokerMethod.setBody(b.toString());
		invokerMethod.setModifiers(invokerMethod.getModifiers() & ~Modifier.ABSTRACT);
		invokerClass.addMethod(invokerMethod);
		//invokerClass.writeFile(System.getProperty("java.io.tmpdir") + File.separator + "jsoninvokers");
		Class<?> clazz = invokerClass.toClass(handlerInstance.getClass().getClassLoader(), handlerInstance.getClass().getProtectionDomain());
		Constructor<?> ctor = clazz.getDeclaredConstructor(Object.class, String.class, String.class, String.class, String.class, RequestType.class);
		AbstractJSONRequestHandlerInvoker invokerInstance = (AbstractJSONRequestHandlerInvoker)ctor.newInstance(handlerInstance, invokerServiceKey, invokerServiceDescription, opName, opDescription, opType);
		subInvokerMap.put(opName, invokerInstance);										
		
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
	
	private static void scanMethods(final Method[] methods, final Map<Class<?>, Map<String, Method>> mappedMethods) {
		for(final Method m: methods) {
			JSONRequestHandler jsonHandler = m.getAnnotation(JSONRequestHandler.class);
			if(jsonHandler!=null) {
				Class<?>[] paramTypes = m.getParameterTypes();
				if(paramTypes.length != 1 || !JSONREQUEST_CLASSES.contains(paramTypes[0])) {
					LOG.warn("Invalid @JSONRequestHandler annotated method [{}]", m.toGenericString());
					continue;
				}
				final Class<?> requestClass = paramTypes[0];
				final String key = m.getName() + "(" + StringHelper.getMethodDescriptor(m) + ")";
				Map<String, Method> map = mappedMethods.get(requestClass);
				if(map==null) {
					map = new HashMap<String, Method>();
					mappedMethods.put(requestClass, map);
				}
				map.put(key, m);
			}
		}		
	}
	
	/**
	 * Finds and returns the valid target {@link JSONRequestHandler} annotated methods in the passed class.
	 * @param clazz the class to inspect
	 * @return a map of target methods keyed by method signature within a map keyed by jsonrequest class
	 */
	public static Map<Class<?>, Map<String, Method>> getTargetMethods(final Class<?> clazz) {
		final Map<Class<?>, Map<String, Method>> mappedMethods = new HashMap<Class<?>, Map<String, Method>>();
		scanMethods(clazz.getMethods(), mappedMethods);
		scanMethods(clazz.getDeclaredMethods(), mappedMethods);
		return mappedMethods;
	}
	
	private static void mapMethod(final HashMap<String, Method[]> map, final Method m, final Class<?> ptype) {
		int index = 2;
		if(JSONRequest.class.equals(ptype)) {
			index = 0;
		} else if(Netty3JSONRequest.class.equals(ptype)) {
			index = 1;
		}
		if(index < 2) {
			Method[] methods = map.get(m.getName());
			if(methods==null) {
				methods = new Method[2];
				map.put(m.getName(), methods);
			}
			methods[index] = m;
		}
	}
	
	/**
	 * Finds and returns the valid target {@link JSONRequestHandler} annotated methods in the passed class.
	 * @param clazz the class to inspect
	 * @return a collection of valid json request methods
	 */
	public static Collection<Method[]> getTargetMethodPairs(final Class<?> clazz) {
		final HashMap<String, Method[]> mappedMethods = new HashMap<String, Method[]>();
		Arrays.stream(clazz.getMethods())
			.filter(m -> m.getParameterTypes().length==1)
			.forEach(m -> mapMethod(mappedMethods, m, m.getParameterTypes()[0]));
		Arrays.stream(clazz.getDeclaredMethods())
			.filter(m -> m.getParameterTypes().length==1)
			.forEach(m -> mapMethod(mappedMethods, m, m.getParameterTypes()[0]));
		
//		for(Method m: clazz.getMethods()) {
//			if(m.getAnnotation(JSONRequestHandler.class)!=null) {
//				final Class<?>[] params = m.getParameterTypes();
//				if(params.length==1) {
//					mapMethod(mappedMethods, m, params[0]);
//				}
//			}
//		}
//		for(Method m: clazz.getDeclaredMethods()) {
//			if(m.getAnnotation(JSONRequestHandler.class)!=null) {
//				final Class<?>[] params = m.getParameterTypes();
//				if(params.length==1) {
//					mapMethod(mappedMethods, m, params[0]);
//				}
//			}
//		}
		
		return mappedMethods.values();
		
	}
	
	
	private JSONRequestHandlerInvokerFactory() {
	}

}
