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

import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heliosapm.webrpc.json.JSONOps;
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
	@JsonIgnore
	private final Object targetService;
	/** The target service name */
	@JsonProperty("svc")
	private final String serviceName;
	/** The target service description */
	@JsonProperty("svcdesc")
	private final String serviceDescription;
	/** The target op name */
	@JsonProperty("op")
	private final String opName;
	
	/** The netty 4 operation type */
	@JsonProperty("netty4Type")
	private final RequestType netty4Type;
	/** The netty 3 operation type */
	@JsonProperty("netty3Type")
	private final RequestType netty3Type;
	
	/** Indicates if Netty 4 callers are supported */
	@JsonIgnore
	private final boolean netty4;
	/** Indicates if Netty 3 callers are supported */
	@JsonIgnore
	private final boolean netty3;

	/** The netty 4 target op description */
	@JsonProperty("netty4Desc")
	private final String netty4OpDescription;
	/** The netty 3 target op description */
	@JsonProperty("netty3Desc")
	private final String netty3OpDescription;
	
	/** The json node describing this invoker */
	@JsonIgnore
	private final ObjectNode descriptor;
	
	private static final String[] NETTY4FIELDS = {"netty4Desc", "netty4Type"};
	private static final String[] NETTY3FIELDS = {"netty3Desc", "netty3Type"};

	
	public static class NettyTypeDescriptor {
		final boolean implemented;
		final String opDescription;
		final RequestType requestType;
		
		private NettyTypeDescriptor(final boolean implemented, final String opDescription, final RequestType requestType) {
			this.implemented = implemented;
			this.opDescription = opDescription;
			this.requestType = requestType;
		}
		
		public static NettyTypeDescriptor descriptor(final boolean implemented, final String opDescription, final RequestType requestType) {
			return new NettyTypeDescriptor(implemented, opDescription.trim(), requestType);
		}
		
	}
	
	/**
	 * Creates a new AbstractJSONRequestHandlerInvoker
	 * @param targetService The target service this invoker is invoking against
	 * @param serviceName The target service name
	 * @param serviceDescription The target service description
	 * @param opName The target op name
	 * @param nettyDescriptors A map of netty type descriptors containing specifics for each netty version supported
	 */
	public AbstractJSONRequestHandlerInvoker(final Object targetService, final String serviceName, final String serviceDescription, final String opName, final Map<Class<?>, NettyTypeDescriptor> nettyDescriptors) {
		this.targetService = targetService;		
		this.serviceName = serviceName;
		this.serviceDescription = serviceDescription;
		this.netty4OpDescription = nettyDescriptors.get(JSONRequest.class).opDescription;
		this.netty3OpDescription = nettyDescriptors.get(Netty3JSONRequest.class).opDescription;
		this.opName = opName;
		this.netty4Type = nettyDescriptors.get(JSONRequest.class).requestType;
		this.netty3Type = nettyDescriptors.get(Netty3JSONRequest.class).requestType;
		this.netty4 = nettyDescriptors.get(JSONRequest.class).implemented;
		this.netty3 = nettyDescriptors.get(Netty3JSONRequest.class).implemented;
		descriptor = (ObjectNode)JSONOps.serializeToNode(this);
		if(!this.netty4) Arrays.stream(NETTY4FIELDS).forEach(f -> descriptor.remove(f));
		if(!this.netty3) Arrays.stream(NETTY3FIELDS).forEach(f -> descriptor.remove(f));		
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
//		ElapsedTime et = SystemClock.startClock();
		try {
			doInvoke(jsonRequest);
//			long elpsd = et.elapsed();
		} catch (Exception ex) {
			
			throw new RuntimeException("Failed to invoke Netty 4 JSON Service [" + serviceName + "/" + opName + "]", ex);
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
//		ElapsedTime et = SystemClock.startClock();
		try {
			doInvoke(jsonRequest);
//			long elpsd = et.elapsed();
		} catch (Exception ex) {
			
			throw new RuntimeException("Failed to invoke Netty 3 JSON Service [" + serviceName + "/" + opName + "]", ex);
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
	@JsonIgnore
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * Returns the target operation name 
	 * @return the opName
	 */
	@JsonIgnore
	public String getOpName() {
		return opName;
	}

	/**
	 * Returns the target service description
	 * @return the serviceDescription
	 */
	@JsonIgnore
	public String getServiceDescription() {
		return serviceDescription;
	}

	/**
	 * Returns the Netty 4 target operation description
	 * @return the Netty 4 opDescription
	 */
	@JsonIgnore
	public String getNetty4OpDescription() {
		return netty4OpDescription;
	}
	
	/**
	 * Returns the Netty 3 target operation description
	 * @return the Netty 3 opDescription
	 */
	@JsonIgnore
	public String getNetty3OpDescription() {
		return netty3OpDescription;
	}
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String
				.format("JSONRequestHandlerInvoker [impl=%s, serviceName=%s, serviceDescription=%s, opName=%s, opDescription=%s]",
						targetService.getClass().getSimpleName(), serviceName, serviceDescription, opName, netty4OpDescription + "|" + netty3OpDescription);
	}
	
	/**
	 * Returns the netty 4 request type of the op
	 * @return the netty 4 request type of the op
	 */
	@JsonIgnore
	public RequestType getNetty4RequestType() {
		return netty4Type;
	}
	
	/**
	 * Returns the netty 3 request type of the op
	 * @return the netty 3 request type of the op
	 */
	@JsonIgnore
	public RequestType getNetty3RequestType() {
		return netty3Type;
	}
	

	/**
	 * Indicates if Netty 4 callers are supported
	 * @return true if Netty 4 callers are supported, false otherwise
	 */
	@JsonIgnore
	public boolean isNetty4() {
		return netty4;
	}


	/**
	 * Indicates if Netty 3 callers are supported
	 * @return true if Netty 3 callers are supported, false otherwise
	 */
	@JsonIgnore
	public boolean isNetty3() {
		return netty3;
	}

	/**
	 * Returns the invoker descriptor as a JSON ObjectNode
	 * @return the descriptor
	 */
	@JsonIgnore
	public ObjectNode getDescriptor() {
		return descriptor;
	}

	/**
	 * Returns the invoker descriptor as a JSON string
	 * @return the descriptor as a string
	 */
	@JsonIgnore
	public String getDescriptorText() {
		return descriptor.toString();
	}
	
	

}
