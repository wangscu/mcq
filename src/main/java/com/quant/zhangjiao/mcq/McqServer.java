package com.quant.zhangjiao.mcq;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.memcache.binary.AbstractBinaryMemcacheDecoder;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheServerCodec;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;

@Service
public class McqServer {
    private final int port;
    private QHandler qHandler;

    public McqServer() {
        this.port = 22212;
        this.qHandler = new QHandler();
    }

    @PostConstruct
    public void start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel sc) throws Exception {
                            sc.pipeline()
                                    .addLast(new BinaryMemcacheServerCodec())
                                    .addLast(new BinaryMemcacheObjectAggregator(AbstractBinaryMemcacheDecoder.DEFAULT_MAX_CHUNK_SIZE))
                                    .addLast(qHandler);
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            channelFuture.channel().closeFuture().sync();
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }
}