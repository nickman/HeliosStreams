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
package com.heliosapm.webrpc.websocket;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.webrpc.json.JSONOps;
import com.heliosapm.webrpc.jsonservice.JSONRequest;
import com.heliosapm.webrpc.jsonservice.JSONRequestRouter;
import com.heliosapm.webrpc.jsonservice.JSONResponse;
import com.heliosapm.webrpc.serialization.ChannelBufferizable;
import com.heliosapm.webrpc.serialization.TSDBTypeSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.CharsetUtil;


/**
 * <p>Title: WebSocketServiceHandler</p>
 * <p>Description: WebSocket handler for fronting JSON based data-services</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.rpc.netty.pipeline.websock.WebSocketServiceHandler</code></p>
 */
@Sharable
public class WebSocketServiceHandler  extends ChannelDuplexHandler {
	/** The JSON Request Router */
	protected final JSONRequestRouter router = JSONRequestRouter.getInstance();
	protected final ObjectMapper marshaller = new ObjectMapper();	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());

	/**
	 * Creates a new WebSocketServiceHandler
	 */
	public WebSocketServiceHandler() {
		marshaller.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
	}

	
	@Override
	public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
		final Channel channel = ctx.channel();
		if(!channel.isOpen() || msg==null) return;
		final TextWebSocketFrame wsf;
		if((msg instanceof HttpResponse) || (msg instanceof WebSocketFrame)) {
			super.write(ctx, msg, promise);
			return;
		} else {
			if(msg instanceof ByteBuf) {
				wsf = new TextWebSocketFrame((ByteBuf)msg);
				super.write(ctx, wsf, promise);
			} else if(msg instanceof JsonNode) {
				wsf = new TextWebSocketFrame(JSONOps.serialize(msg));
				super.write(ctx, wsf, promise);			
			} else if(msg instanceof ChannelBufferizable) {
				wsf = new TextWebSocketFrame(((ChannelBufferizable)msg).toByteBuf());
				super.write(ctx, wsf, promise);						
			} else if(msg instanceof CharSequence) {
				wsf = new TextWebSocketFrame(marshaller.writeValueAsString(msg.toString()));
				super.write(ctx, wsf, promise);			
			} else if(msg instanceof JSONResponse) {
				ObjectMapper mapper = (ObjectMapper)((JSONResponse)msg).getChannelOption("mapper", TSDBTypeSerializer.DEFAULT.getMapper());
				wsf = new TextWebSocketFrame(mapper.writeValueAsString(msg));									
			} else {
				wsf = null;
				channelRead(ctx, msg);
			}
			if(wsf!=null) super.write(ctx, wsf, promise);			
		} 
	}
	
	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		if (msg instanceof FullHttpRequest) {
			handleRequest(ctx, (FullHttpRequest)msg);
		} else if (msg instanceof WebSocketFrame) {
			handleRequest(ctx, (WebSocketFrame)msg);
		} else {
			super.channelRead(ctx, msg);
		}
	}
	
	
	/**
	 * Handles uncaught exceptions in the pipeline from this handler
	 * @param ctx The channel context
	 * @param ev The exception event
	 */
	
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable ev) {
		log.error("Uncaught exception ", ev.getCause());
	}
	
	
	
	/**
	 * Processes a websocket request
	 * @param ctx The channel handler context
	 * @param frame The websocket frame request to process
	 */
	public void handleRequest(final ChannelHandlerContext ctx, final WebSocketFrame frame) {
		final Channel channel = ctx.channel();
		final JSONRequest wsRequest = JSONRequest.newJSONRequest(channel, frame.content());
		router.route(wsRequest);
	}
	
	/**
	 * Processes an HTTP request
	 * @param ctx The channel handler context
	 * @param req The HTTP request
	 */
	public void handleRequest(final ChannelHandlerContext ctx, final FullHttpRequest req) {
		log.warn("HTTP Request: {}", req);
        if (req.method() != GET) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }
        String uri = req.uri();
        if(!"/ws".equals(uri)) {
        	//channelRead(ctx, req);
        	final WebSocketServerProtocolHandler wsProto = ctx.pipeline().get(WebSocketServerProtocolHandler.class);
        	if(wsProto != null) {
        		try {
					wsProto.acceptInboundMessage(req);
					return;
				} catch (Exception ex) {
					log.error("Failed to dispatch http request to WebSocketServerProtocolHandler on channel [{}]", ctx.channel(), ex);
				}
        	}
        }
        log.error("Failed to handle HTTP Request [{}] on channel [{}]", req, ctx.channel());
        sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.MISDIRECTED_REQUEST));
	}
	
    /**
     * Generates a websocket URL for the passed request
     * @param req The http request
     * @return The websocket URL
     */
    private String getWebSocketLocation(HttpRequest req) {
        return "ws://" + req.headers().get(HttpHeaderNames.HOST) + "/ws";
    }	
	
    private static void sendHttpResponse(final ChannelHandlerContext ctx, final FullHttpRequest req, final FullHttpResponse res) {
    	// Generate an error page if response getStatus code is not OK (200).
    	if (res.status().code() != 200) {
    		ByteBuf buf = Unpooled.copiedBuffer(res.status().toString(), CharsetUtil.UTF_8);
    		res.content().writeBytes(buf);
    		buf.release();
    		HttpUtil.setContentLength(res, res.content().readableBytes());
    	}

    	// Send the response and close the connection if necessary.
    	ChannelFuture f = ctx.channel().writeAndFlush(res);
    	if (!HttpUtil.isKeepAlive(req) || res.status().code() != 200) {
    		f.addListener(ChannelFutureListener.CLOSE);
    	}
    }
    
    
//	@Override
//	public void execute(TSDB tsdb, HttpQuery query) throws IOException {
//		if(query.getAPIMethod()!=GET) {
//			query.badRequest("HTTP Request Type [" + query.getAPIMethod() + "] Forbidden");
//			return;
//		}
//        
//        final Channel channel = query.channel();
//        final  HttpRequest req = query.request();
//        String uri = req.getUri();
//        final WebSocketServiceHandler wsHandler = this;
//        // Handshake
//        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(getWebSocketLocation(req), null, false);
//        WebSocketServerHandshaker handshaker = wsFactory.newHandshaker(req);
//        if (handshaker == null) {
//            wsFactory.sendUnsupportedWebSocketVersionResponse(channel);
//        } else {
//        	wsHandShaker.set(channel, handshaker);
//        	ChannelFuture cf = handshaker.handshake(channel, req); 
//            cf.addListener(WebSocketServerHandshaker.HANDSHAKE_LISTENER);
//            cf.addListener(new ChannelFutureListener() {
//				@Override
//				public void operationComplete(ChannelFuture f) throws Exception {
//					if(f.isSuccess()) {
//						Channel wsChannel = f.getChannel();
//						wsChannel.getPipeline().addLast("websock", wsHandler);
//						RPCSessionManager.getInstance().getSession(wsChannel).addSessionAttribute(RPCSessionAttribute.Protocol, "WebSocket");
////						SharedChannelGroup.getInstance().add(
////								f.getChannel(), 
////								ChannelType.WEBSOCKET_REMOTE, 
////								"WebSocketClient-" + f.getChannel().getId(), 
////								((InetSocketAddress)wsChannel.getRemoteAddress()).getAddress().getCanonicalHostName(), 
////								"WebSock[" + wsChannel.getId() + "]"
////						);
//						//wsChannel.write(new JSONObject(Collections.singletonMap("sessionid", wsChannel.getId())));
//						
//						wsChannel.write(marshaller.getNodeFactory().objectNode().put("sessionid", "" + wsChannel.getId()));
//						//wsChannel.getPipeline().remove(DefaultChannelHandler.NAME);
//					}
//				}
//			});
//        }
//		
//	}
    

}
