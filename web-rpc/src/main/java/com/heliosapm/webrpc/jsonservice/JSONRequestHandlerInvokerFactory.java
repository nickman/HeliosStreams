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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
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
			.recordStats()
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
			final String invokerServiceDescription = jrs.description();
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
			final Map<Class<?>, NettyTypeDescriptor> descriptors = new HashMap<Class<?>, NettyTypeDescriptor>();
			// ==== implement the doInvoke methods
			for(final Map.Entry<Class<?>, Integer> entry: jsonRequestIndexes.entrySet()) {
				final int index = entry.getValue();
				final Class<?> jsonRequestType = entry.getKey();
				final Method gm = methods[index];				
				if(gm==null) {
					descriptors.put(jsonRequestType, NettyTypeDescriptor.descriptor(false, jsonRequestTypeNames[index] + " Not Implemented", null));
					generateNoOpCtMethod(jsonRequestType, invokerClass);
				} else {
					final JSONRequestHandler jrh = gm.getAnnotation(JSONRequestHandler.class);
					descriptors.put(jsonRequestType, NettyTypeDescriptor.descriptor(true, jrh.description() + " (" + jsonRequestTypeNames[index] + ")", jrh.type()));
					generateCtMethod(jsonRequestType, gm, invokerClass);
				}
			}
			invokerClass.writeFile("/tmp/webrpc");
			// ==== build the class and instantiate
			final Class<?> clazz = invokerClass.toClass(handlerType.getClassLoader(), handlerType.getProtectionDomain());
//			public AbstractJSONRequestHandlerInvoker(
//					final Object targetService, 
//					final String serviceName, 
//					final String serviceDescription, 
//					final String opName, 
//					final Map<Class<?>, NettyTypeDescriptor> nettyDescriptors) 
			final Constructor<?> ctor = clazz.getDeclaredConstructor(Object.class, String.class, String.class, String.class, Map.class);
			final AbstractJSONRequestHandlerInvoker invokerInstance = (AbstractJSONRequestHandlerInvoker)
				ctor.newInstance(handlerInstance, invokerServiceKey, invokerServiceDescription, opName, descriptors);
			return invokerInstance;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to generate invoker for type [" + handlerType.getSimpleName() + "], op: [" + opName + "]", ex);
		}			
	}
	
	private static void generateNoOpCtMethod(final Class<?> jsonRequestType, final CtClass invokerClass) throws CannotCompileException, NotFoundException {
		final CtMethod noOpMethod = CtNewMethod.copy(parent.getDeclaredMethod("doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}), invokerClass, null); 
				//new CtMethod(CtClass.voidType, "doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}, invokerClass);
		noOpMethod.setBody("{throw new " + UnsupportedOperationException.class.getName() + "();}");
		noOpMethod.setModifiers(noOpMethod.getModifiers() & ~Modifier.ABSTRACT);
		invokerClass.addMethod(noOpMethod);
	}
	
	private static void generateCtMethod(final Class<?> jsonRequestType, final Method m, final CtClass invokerClass) throws CannotCompileException, NotFoundException {
		final CtMethod opMethod = CtNewMethod.copy(
				parent.getDeclaredMethod("doInvoke", new CtClass[]{javaToCtMap.get(jsonRequestType)}), 
				invokerClass, null);
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
		try {
			return invokerCache.get(clazz, new Callable<Map<String, Map<String, AbstractJSONRequestHandlerInvoker>>>(){
				@Override
				public Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> call() throws Exception {				
					LOG.info("Generating Invokers for [{}]", clazz.getName());
					final JSONRequestService requestService = clazz.getAnnotation(JSONRequestService.class);
					final String serviceName = requestService.name();
					final Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> invokerMap = new ConcurrentHashMap<String, Map<String, AbstractJSONRequestHandlerInvoker>>();
					final Map<Class<?>, Map<String, Method>> targetMethods = getTargetMethods(clazz);
					if(targetMethods.isEmpty()) return EMPTY_INVOKER_MAP;
					final Map<String, Method[]> methodPairsByOp = mapByOp(targetMethods);
					for(final Map.Entry<String, Method[]> entry: methodPairsByOp.entrySet()) {
						final String opName = entry.getKey();
						final Method[] methodSet = entry.getValue();
						final AbstractJSONRequestHandlerInvoker invoker = generate(handlerInstance, opName, methodSet);
						final Map<String, AbstractJSONRequestHandlerInvoker> subInvokerMap = invokerMap.computeIfAbsent(invoker.getServiceName(), k -> 
							new ConcurrentHashMap<String, AbstractJSONRequestHandlerInvoker>()
						);
						subInvokerMap.put(opName, invoker);
					}
					return invokerMap;
				}
			});
		} catch (ExecutionException ex) {
			final String msg = "Failed to create invokers for handler [" + clazz.getName() + "]";
			LOG.error(msg, ex);
			throw new RuntimeException(msg, ex);
		}
	}
	
	
	
	public static void main(String[] args) {
		System.out.println("Cache Size:" + invokerCache.size());
		final ElapsedTime et = SystemClock.startClock();		
		createInvokers(new FooService());
		System.out.println("Cache Size:" + invokerCache.size());
		System.out.println("First Invoke:" + et.printAvg("", 1));
		final Map<String, Map<String, AbstractJSONRequestHandlerInvoker>> invokers = createInvokers(new FooService());
		System.out.println("Cache Size:" + invokerCache.size());
		System.out.println("Second Invoke:" + et.printAvg("", 1));
		System.out.println("Cache Stats:" + invokerCache.stats());
		System.out.println("=====================");
		invokers.values().stream()
			.map(m -> m.values())
			.forEach(c -> {
				c.forEach(inv -> System.out.println("Descriptor:" + inv.getDescriptorText()));
			});
			
			
		
		// System.out.println("Descriptor:" + inv.getDescriptorText())
		
		
	}
	
	@JSONRequestService(name="foo")
	public static class FooService {
		@JSONRequestHandler(name="bar1")
		public void bar1(JSONRequest request) {
			
		}
		@JSONRequestHandler(name="bar0")
		public void bar0(JSONRequest request) {
			
		}
		@JSONRequestHandler(name="bar3")
		public void bar3(JSONRequest request) {
			
		}
		
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
	
	
	
	private JSONRequestHandlerInvokerFactory() {
	}

}
