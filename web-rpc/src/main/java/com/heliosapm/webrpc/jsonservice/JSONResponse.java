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

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.webrpc.serialization.ChannelBufferizable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.AttributeKey;
import net.opentsdb.utils.JSON;

/**
 * <p>Title: JSONResponse</p>
 * <p>Description:  The standard object container for sending a response to a JSON data service caller</p>
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><b><code>com.heliosapm.webrpc.jsonservice.JSONResponse</code></b>
 */

public class JSONResponse implements ChannelBufferizable {
	/** The client provided request ID that this response is being sent for */
	@JsonProperty("rerid")
	protected final long reRequestId;
	/** The response type */
	@JsonProperty("t")
	protected final String type;
	/** The response instance id */
	@JsonProperty("id")
	protected final int id = System.identityHashCode(this);
	
	/** The content payload */
	@JsonProperty("msg")
	//@JsonSerialize(using = JSONByteBufSerializer.class, as=Object.class)
	protected Object content = null;
	
	/** An object mapper override for custom/dynamic JSON serialization */
	@JsonIgnore
	protected volatile ObjectMapper mapperOverride = null;
	
	@JsonIgnore
	protected final JSONRequest parentRequest;
	
	
	
	/** The response op code */
	@JsonProperty("op")
	protected String opCode = null;
	
	/** The channel that the request came in on. May sometimes be null */
	@JsonIgnore
	public final Channel channel;
	
	/** The channel output stream used in lieu of content */
	@JsonIgnore
	private volatile ByteBufOutputStream channelOutputStream = null;
	
	/** The json generator for streaming content */
	@JsonIgnore
	private volatile JsonGenerator jsonGen = null;
	
	/** Indicates if the json generator was opened as a map (true) or an array (false) */
	@JsonIgnore
	private boolean openedAsMap = true;
	
	/** The shared json mapper */
	private static final ObjectMapper jsonMapper = new ObjectMapper();	
	

	/** An empty ChannelFuture const. */
	private static final ChannelFuture[] EMPTY_CHANNEL_FUTURE_ARR = {};
	
	
	
	/**
	 * Clones this json response with a new type
	 * @param type the new type
	 * @return an updated type clone of this response
	 * @param parent The parent request for this response
	 */
	public JSONResponse clone(final ResponseType type, final JSONRequest parent) {
		return new JSONResponse(reRequestId, type, channel, parent);
	}
	
	/**
	 * Creates a new JSONResponse
	 * @param reRequestId The client provided request ID that this response is being sent for
	 * @param type The type flag. Currently "err" for an error message, "resp" for a response, "sub" for subcription event
	 * @param channel The channel this response will be written to 
	 * @param parent The parent request for this response
	 */
	public JSONResponse(final long reRequestId, final ResponseType type, final Channel channel, final JSONRequest parent) {
		super();
		this.reRequestId = reRequestId;
		this.type = type.code;
		this.channel = channel;
		this.parentRequest = parent;
	}
	
	/**
	 * Sets the override ObjectMapper
	 * @param mapper the override ObjectMapper
	 * @return this JSONResponse
	 */
	public JSONResponse setOverrideObjectMapper(ObjectMapper mapper) {
		this.mapperOverride = mapper;
		return this;
	}
	
	/**
	 * Returns an OutputStream that writes directly to a channel buffer which will be flushed to the channel on send
	 * @return a channel buffer OutputStream 
	 */
	public OutputStream getChannelOutputStream() {
		if(content!=null) {
			throw new RuntimeException("Cannot start OutputStream. Content already set");
		}
		if(channelOutputStream==null) {
			channelOutputStream = new ByteBufOutputStream(BufferManager.getInstance().buffer(8096)) {
				final ByteBuf buf = this.buffer();
				boolean closed = false;
				@Override
				public void close() throws IOException {
					if(!closed) {
						closed = true;
						super.flush();					
						super.close();
						channel.write(buf);
					}
				}
				
				public void flush() throws IOException {
					super.flush();
					super.close();
				}
			};			
		}
		return channelOutputStream;
	}
	
	/**
	 * Discards written content from the output stream, discarding the content and creating a new output stream
	 */
	public void resetChannelOutputStream() {
		if(content!=null) {
			throw new RuntimeException("Cannot reset OutputStream. Content already set");
		}
		if(channelOutputStream != null) {
			ByteBuf buff = channelOutputStream.buffer();
			try {
				channelOutputStream.flush();
			} catch (Exception ex) {}
			buff.resetWriterIndex();
			channelOutputStream = new ByteBufOutputStream(BufferManager.getInstance().buffer(8096)) {
				final ByteBuf buf = this.buffer();
				boolean closed = false;
				@Override
				public void close() throws IOException {
					if(!closed) {
						closed = true;
						super.flush();					
						super.close();
						channel.write(buf);
					}
				}
				
				public void flush() throws IOException {
					super.flush();
					super.close();
				}
			};						
			jsonGen = null;
//			try {
//				jsonGen = JSON.getFactory().createGenerator(channelOutputStream);
//			} catch (IOException e) {
//				throw new RuntimeException("Failed to create a new JsonGenerator after reset", e);
//			}
		}
	}
	
	/**
	 * Returns the content payload
	 * @return the content
	 */
	public Object getContent() {
		return content;
	}
	
	
	/**
	 * Sets a channel option
	 * @param name The option name
	 * @param value The option value
	 * @return this JSONResponse
	 */
	public JSONResponse setChannelOption(final String name, final Object value) {
		channel.attr(AttributeKey.valueOf(name)).setIfAbsent(value);
		return this;
	}
	
	/**
	 * Clears the channel options
	 * @return this JSONResponse
	 */
	public JSONResponse clearChannelOptions() {
		// FIXME
		return this;
	}
	
	/**
	 * Removes a channel option
	 * @param name The name of the option to remove
	 * @return this JSONReponse
	 */
	public JSONResponse removeChannelOption(final String name) {
		channel.attr(AttributeKey.valueOf(name)).set(null);
		return this;
	}		

	/**
	 * Retrieves a channel option
	 * @param name The name of the option to retrieve
	 * @param defaultOption The default value to return if the named option was not bound.
	 * @return the named channel option or the defalt if it was not found.
	 */
	public Object getChannelOption(final String name, final Object defaultOption) {
		Object value = channel.attr(AttributeKey.valueOf(name)).get(); 
		return value!=null ? value : defaultOption;
	}		

	/**
	 * Retrieves a channel option
	 * @param name The name of the option to retrieve
	 * @return the named channel option or null if it was not found.
	 */
	public Object getChannelOption(final String name) {
		return getChannelOption(name, null);
	}
	

	/**
	 * Sets the payload content
	 * @param content the content to set
	 * @return this json response
	 */
	public JSONResponse setContent(Object content) {
		if(channelOutputStream!=null) {
			throw new RuntimeException("Cannot set content. OutputStream already set");
		}
		this.content = content;
		return this;
	}
	
	/**
	 * Sets the payload content from the passed buffer
	 * @param channelBuffer The buffer to set the content to
	 * @return this json response
	 */
	public JSONResponse setContent(ByteBuf channelBuffer) {
		if(channelOutputStream!=null) {
			throw new RuntimeException("Cannot set content. OutputStream already set");
		}
		content = channelBuffer;
		//channelOutputStream = new ByteBufOutputStream(channelBuffer);
		return this;
	}
	
	@SuppressWarnings("unused")
	private static String printOutputContext(JsonStreamContext ctx) {
		return new StringBuilder("JSONContext [")
		.append("\n\tCurrent Index:").append(ctx.getCurrentIndex())
		.append("\n\tCurrent Name:").append(ctx.getCurrentName())
		.append("\n\tEntry Count:").append(ctx.getEntryCount())
		.append("\n\tType Desc:").append(ctx.getTypeDesc())
		.append("\n\tIn Array:").append(ctx.inArray())
		.append("\n\tIn Object:").append(ctx.inObject())
		.append("\n\tIn Root:").append(ctx.inRoot())
		.append("\n]").toString();
		
	}
	
	
	/**
	 * Initiates a streaming content delivery to the caller. A new JsonFactory is created and the header of 
	 * the response message is written, up to an including the msg object start. The remaining content
	 * should be written using the returned generator, followed by a call to closeGenerator.
	 * @param map True if the payload body is a map, false for an array
	 * @return the created generator.
	 */
	public JsonGenerator writeHeader(final boolean map) {
		if(jsonGen!=null) throw new RuntimeException("The json generator has already been set");
		try {
			openedAsMap = map;
			jsonGen = JSON.getFactory().createGenerator(getChannelOutputStream());
			jsonGen.writeStartObject();
			jsonGen.writeNumberField("id", id);
			jsonGen.writeNumberField("rerid", reRequestId);
			jsonGen.writeStringField("t", type);
			jsonGen.writeStringField("op", opCode);
			
			if(openedAsMap) {
				jsonGen.writeObjectFieldStart("msg");
			} else {
				jsonGen.writeArrayFieldStart("msg");
			}
			return jsonGen;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create JsonGenerator", ex);
		}
	}
	
	
	/**
	 * Closes the open json generator and writes the content back to the caller.
	 */
	public void closeGenerator() {
		if(jsonGen==null || jsonGen.isClosed()) throw new RuntimeException("The json generator " + (jsonGen==null ? "is null" : "has already been closed"));
		try {
			if(openedAsMap) {
				jsonGen.writeEndObject();
			} else {
				jsonGen.writeEndArray();
			}
//			System.out.println(printOutputContext(jsonGen.getOutputContext())) ;
			jsonGen.writeEndObject();
			jsonGen.close();
			channelOutputStream.close();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to close JsonGenerator", ex);
		} finally {
			jsonGen = null;
			channelOutputStream = null;
			content = null;
		}		
	}

	/**
	 * Returns the in reference to request id
	 * @return the in reference to request id
	 */
	public long getReRequestId() {
		return reRequestId;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.webrpc.serialization.helios.tsdb.plugins.remoting.json.ByteBufizable#toByteBuf()
	 */
	@Override
	public ByteBuf toByteBuf() {
		try {
			if(channelOutputStream!=null) {
				channelOutputStream.flush();
				return channelOutputStream.buffer();
			}
			ObjectMapper om = mapperOverride;
			if(om==null) {
				om = jsonMapper;
			}
			return JSONOps.serialize(this);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write object as JSON bytes", ex);
		}
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.webrpc.serialization.helios.tsdb.plugins.remoting.json.ByteBufizable#write(org.jboss.netty.buffer.ByteBuf)
	 */
	@Override
	public void write(final ByteBuf buffer) {
		ObjectMapper om = mapperOverride;
		if(om==null) {
			om = jsonMapper;
		}			
		JSONOps.serialize(this, buffer);
	}


	/**
	 * Returns the type flag
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * Returns the response op code
	 * @return the response op code
	 */
	public String getOpCode() {
		return opCode;
	}

	/**
	 * Sets the response op code
	 * @param opCode the response op code
	 * @return this response
	 */
	public JSONResponse setOpCode(String opCode) {
		this.opCode = opCode;
		return this;
	}
	
	
	/**
	 * Sends this response to all the passed channels as a {@link TextWebSocketFrame}
	 * @param listener A channel future listener to attach to each channel future. Ignored if null.
	 * @param channels The channels to send this response to
	 * @return An array of the futures for the write of this response to each channel written to
	 */
	public ChannelFuture[] send(ChannelFutureListener listener, Channel...channels) {
		if(channels!=null && channels.length>0) {
			Set<ChannelFuture> futures = new HashSet<ChannelFuture>(channels.length);
			if(opCode==null) {
				opCode = "ok";
			}
			TextWebSocketFrame frame = new TextWebSocketFrame(this.toByteBuf());
			for(Channel channel: channels) {
				if(channel!=null && channel.isWritable()) {
					ChannelFuture cf = channel.pipeline().writeAndFlush(frame);
					if(listener!=null) cf.addListener(listener);
					futures.add(cf);
				}
			}
			return futures.toArray(new ChannelFuture[futures.size()]);
		}		
		return EMPTY_CHANNEL_FUTURE_ARR;
	}
	
	/**
	 * Sends this response to all the passed channels as a {@link TextWebSocketFrame}
	 * @param channels The channels to send this response to
	 * @return An array of the futures for the write of this response to each channel written to
	 */
	public ChannelFuture[] send(Channel...channels) {
		return send(null, channels);
	}
	
	/**
	 * Sends this response to the default channel
	 * @return the channel future for the send
	 */
	public ChannelFuture[] send() {
		return send(channel);
	}
	
}
