package com.phantom.im.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;

/**
 * @description:
 * @author: 高露 lugao2
 * @create: 2025/4/27
 * @Version 1.0.0
 */
@Slf4j
public class WebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
        String request = msg.text();
        if (log.isDebugEnabled()) {
            log.debug("Received message: {}", request);
        }
        ctx.channel().writeAndFlush(new TextWebSocketFrame("Server received: " + request));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Websocket Error: {}", cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}

