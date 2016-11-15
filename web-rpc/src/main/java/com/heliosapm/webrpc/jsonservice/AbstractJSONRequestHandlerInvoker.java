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

import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.heliosapm.webrpc.jsonservice.netty3.Netty3JSONRequest;

/**
 * <p>Title: AbstractJSONRequestHandlerInvoker</p>
 * <p>Description: An abstract and instrumented wrapper for generated json invokers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.jsonservice.AbstractJSONRequestHandlerInvoker</code></p>
 */

public abstract class AbstractJSONRequestHandlerInvoker  {
	/** The target service this invoker is invoking against */
	private final Object targetService;
	/** The target service name */
	private final String serviceName;
	/** The target op name */
	private final String opName;
	/** The target service description */
	private final String serviceDescription;
	/** The target op description */
	private final String opDescription;
	/** The operation type */
	private final RequestType type;
	/** Indicates if Netty 4 callers are supported */
	private final boolean netty4;
	/** Indicates if Netty 3 callers are supported */
	private final boolean netty3;
	
	
	/**
	 * Creates a new AbstractJSONRequestHandlerInvoker
	 * @param targetService The target service this invoker is invoking against
	 * @param serviceName The target service name
	 * @param serviceDescription The target service description
	 * @param opName The target op name
	 * @param opDescription The target op description
	 * @param type The op type
	 * @param netty4 true if Netty 4 callers are supported, false otherwise
	 * @param netty3 true if Netty 3 callers are supported, false otherwise
	 */
	public AbstractJSONRequestHandlerInvoker(Object targetService, String serviceName, String serviceDescription, String opName, String opDescription, RequestType type, final boolean netty4, final boolean netty3) {
		this.targetService = targetService;		
		this.serviceName = serviceName;
		this.serviceDescription = serviceDescription;
		this.opDescription = opDescription;
		this.opName = opName;
		this.type = type;
		this.netty3 = netty3;
		this.netty4 = netty4;
	}
	

	


	/**
	 * Invokes the passed json request
	 * @param jsonRequest The json request to invoke
	 */
	public void invokeJSONRequest(JSONRequest jsonRequest) {
		if(!netty4) {
			jsonRequest.error("Netty4 Clients Not Supported By This Endpoint");
			return;
		}
		ElapsedTime et = SystemClock.startClock();
		try {
			doInvoke(jsonRequest);
			long elpsd = et.elapsed();
		} catch (Exception ex) {
			
			throw new RuntimeException("Failed to invoke JSON Service [" + serviceName + "/" + opName + "]", ex);
		}
	}
	
	/**
	 * Invokes the passed json request
	 * @param jsonRequest The json request to invoke
	 */
	public void invokeJSONRequest(final Netty3JSONRequest jsonRequest) {
		if(!netty3) {
			jsonRequest.error("Netty3 Clients Not Supported By This Endpoint");
			return;
		}
		ElapsedTime et = SystemClock.startClock();
		try {
			doInvoke(jsonRequest);
			long elpsd = et.elapsed();
		} catch (Exception ex) {
			
			throw new RuntimeException("Failed to invoke JSON Service [" + serviceName + "/" + opName + "]", ex);
		}
	}
	
	
	
	
	/**
	 * The byte-code generated json request invoker
	 * @param jsonRequest the request to invoke
	 */
	public abstract void doInvoke(final JSONRequest jsonRequest);
	
	/**
	 * The byte-code generated json request invoker
	 * @param jsonRequest the request to invoke
	 */
	public abstract void doInvoke(final Netty3JSONRequest jsonRequest);
	

	/**
	 * Returns the target service name
	 * @return the service name
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * Returns the target operation name 
	 * @return the opName
	 */
	public String getOpName() {
		return opName;
	}

	/**
	 * Returns the target service description
	 * @return the serviceDescription
	 */
	public String getServiceDescription() {
		return serviceDescription;
	}

	/**
	 * Returns the target operation description
	 * @return the opDescription
	 */
	public String getOpDescription() {
		return opDescription;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String
				.format("JSONRequestHandlerInvoker [impl=%s, serviceName=%s, serviceDescription=%s, opName=%s, opDescription=%s]",
						targetService.getClass().getSimpleName(), serviceName, serviceDescription, opName, opDescription);
	}
	
	/**
	 * Returns the request type of the op
	 * @return the request type of the op
	 */
	public RequestType getRequestType() {
		return type;
	}

	/**
	 * Indicates if Netty 4 callers are supported
	 * @return true if Netty 4 callers are supported, false otherwise
	 */
	public boolean isNetty4() {
		return netty4;
	}


	/**
	 * Indicates if Netty 3 callers are supported
	 * @return true if Netty 3 callers are supported, false otherwise
	 */
	public boolean isNetty3() {
		return netty3;
	}

	
	

}
