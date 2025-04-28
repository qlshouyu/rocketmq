package com.phantom.im.websocket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

import java.net.InetSocketAddress;

/**
 * @description:
 * @author: 高露 lugao2
 * @create: 2025/4/27
 * @Version 1.0.0
 */
@Slf4j
public class WSRemotingServer extends NettyRemotingServer {

    public WSRemotingServer(NettyServerConfig nettyServerConfig) {
        super(nettyServerConfig);
    }

    public WSRemotingServer(NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        super(nettyServerConfig, channelEventListener);
    }

    @Override
    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        super.initServerBootstrap(serverBootstrap);

        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(),
                        this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        WSRemotingServer.this.configChannel(ch);
                    }
                });

        addCustomConfig(serverBootstrap);
    }

    @Override
    protected ChannelPipeline configChannel(SocketChannel ch) {
        return ch.pipeline().addLast(new HttpServerCodec()) // HTTP 编解码器
        .addLast(new ChunkedWriteHandler()) // 大块数据的处理器
        .addLast(new HttpObjectAggregator(65536)) // HTTP 消息聚合器，用以将多个消息组合为单一的FullHttpRequest或FullHttpResponse对象。
        .addLast(new WebSocketServerProtocolHandler("/phantom/im/ws")) // WebSocket协议处理器，指定访问路径为 /ws
        .addLast(new WebSocketFrameHandler()); // 自定义的WebSocket帧处理器，用于处理文本消息和二进制消息等。
    }
}
