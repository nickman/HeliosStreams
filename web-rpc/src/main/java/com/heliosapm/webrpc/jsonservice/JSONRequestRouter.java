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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heliosapm.webrpc.websocket.annotations.JSONRequestHandler;
import com.heliosapm.webrpc.websocket.annotations.JSONRequestService;


/**
 * <p>Title: JSONRequestRouter</p>
 * <p>Description: Examines JSON requests and routes them to the correct {@link JSONRequestService} annotated instance.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.jsonservice.JSONRequestRouter</code></p>
 */
@JSONRequestService(name="router", description="The main JSON request routing service")
public class JSONRequestRouter {
	/** The singleton instance */
	protected static volatile JSONRequestRouter instance = null;
	/** The singleton instance ctor lock */
	protected static final Object lock = new Object();	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The invoker map */
	protected final ConcurrentHashMap<String, Map<String, AbstractJSONRequestHandlerInvoker>> invokerMap = new ConcurrentHashMap<String, Map<String, AbstractJSONRequestHandlerInvoker>>();
	/** The json node factory */
	private final JsonNodeFactory nodeFactory = JsonNodeFactory.instance; 
	
	
	/**
	 * Acquires and returns the singleton instance
	 * @return the singleton instance
	 */
	public static JSONRequestRouter getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new JSONRequestRouter(); 
				}
			}
		}
		return instance;
	}

	/**
	 * Creates a new JSONRequestRouter
	 */
	private JSONRequestRouter() {
		registerJSONService(this);
		//FIXME
//		registerJSONService(new SystemJSONServices());
//		registerJSONService(new TSDBJSONService());
	}
	
	/**
	 * Registers a new JSON Service which are pojos 
	 * @param service An object annotated with @JSONRequestService and @JSONRequestHandler annotations.
	 */
	public void registerJSONService(Object service) {
		if(service==null) throw new IllegalArgumentException("The passed JSON Service was null");
		for(Map.Entry<String, Map<String, AbstractJSONRequestHandlerInvoker>> entry: JSONRequestHandlerInvokerFactory.createInvokers(service).entrySet()) {
			invokerMap.putIfAbsent(entry.getKey(), entry.getValue());
			log.info("Added [{}] JSONRequest Operations for Service [{}] from impl [{}]", entry.getValue().size(), entry.getKey(), service.getClass().getName());
		}
		StringBuilder b = new StringBuilder("\n\t=========================================================\n\tJSONRequestRouter Routes\n\t=========================================================");
		for(Map.Entry<String, Map<String, AbstractJSONRequestHandlerInvoker>> serviceEntry: invokerMap.entrySet()) {
			b.append("\n\t").append(serviceEntry.getKey());
			for(String key: serviceEntry.getValue().keySet()) {
				b.append("\n\t\t").append(key);
			}
		}						
		b.append("\n\t=========================================================\n");
		log.info(b.toString());
		
	}
	
	/**
	 * Routes a json request to the intended request handler
	 * @param jsonRequest The request to route
	 */
	public void route(JSONRequest jsonRequest) {
		Map<String, AbstractJSONRequestHandlerInvoker> imap = invokerMap.get(jsonRequest.serviceName);
		if(imap==null) {
			jsonRequest.error("Failed to route to service name [" + jsonRequest.serviceName + "]").send();
			return;
		}
		AbstractJSONRequestHandlerInvoker invoker = imap.get(jsonRequest.opName);
		if(invoker==null) {
			jsonRequest.error("Failed to route to op [" + jsonRequest.serviceName + "/" + jsonRequest.opName + "]").send();
			return;
		}
		invoker.invokeJSONRequest(jsonRequest);		
	}
	
	/**
	 * Writes a JSON catalog of the available services
	 * @param jsonRequest The json request
	 * <p>Note: payload for test:<b><code>{"t":"req", "rid":1, "svc":"router", "op":"services"}</code></b></p>
	 */
	@JSONRequestHandler(name="services", description="Returns a catalog of available JSON services")
	public void services(JSONRequest jsonRequest) {
		ObjectNode servicesMap = nodeFactory.objectNode();
		ObjectNode serviceMap = nodeFactory.objectNode();
		servicesMap.put("services", serviceMap);
		for(Map.Entry<String, Map<String, AbstractJSONRequestHandlerInvoker>> entry: invokerMap.entrySet()) {
			Map<String, AbstractJSONRequestHandlerInvoker> opInvokerMap = entry.getValue();
			if(opInvokerMap.isEmpty()) continue;
			ObjectNode svcMap = nodeFactory.objectNode();
			serviceMap.put(entry.getKey(), svcMap);
			svcMap.put("desc", opInvokerMap.values().iterator().next().getServiceDescription());			
			ObjectNode opMap = nodeFactory.objectNode();
			svcMap.put("ops", opMap);
			
			for(AbstractJSONRequestHandlerInvoker invoker: opInvokerMap.values()) {
				ObjectNode opDetails = nodeFactory.objectNode();
				opDetails.put("desc", invoker.getOpDescription());
				opDetails.put("type", invoker.getRequestType().code);				
				opMap.put(invoker.getOpName(), opDetails);
			}			
		}
		try {			
			jsonRequest.response(ResponseType.RESP).setContent(servicesMap).send();
		} catch (Exception ex) {
			log.error("Failed to write service catalog", ex);
			jsonRequest.error("Failed to write service catalog", ex).send();
		}
	}

}
