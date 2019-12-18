package com.quant.zhangjiao.mcq;

import com.quant.zhangjiao.queue.SQueue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.memcache.binary.*;
import io.netty.util.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
public class QHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(QHandler.class);

    private final static int maxValueSize = 1018;
    private ConcurrentHashMap<String, SQueue> qMap = new ConcurrentHashMap<>();
    final HashedWheelTimer timer = new HashedWheelTimer();

    public QHandler() {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DefaultFullBinaryMemcacheRequest) {
            threadPoolExecutor.submit(() -> {
                try {
                    DefaultFullBinaryMemcacheRequest request = (DefaultFullBinaryMemcacheRequest) msg;
                    switch (request.opcode()) {
                        case BinaryMemcacheOpcodes.GETK:
                            handleGet(ctx, request);
                            break;
                        case BinaryMemcacheOpcodes.SET:
                            handleSet(ctx, request);
                            break;
                        case BinaryMemcacheOpcodes.STAT:
                            stats(ctx, request);
                            break;
                        default:
                            ctx.close();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    ctx.close();
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            });
        }
    }

    private void handleGet(final ChannelHandlerContext ctx, DefaultFullBinaryMemcacheRequest request) throws Exception {
        String key = request.key().toString(CharsetUtil.UTF_8);
        SQueue sQueue = getQ(key);
        String val = sQueue.get();
        if (null == val) {
            String msg = "Unable to find Key: " + key;
            DefaultFullBinaryMemcacheResponse nullResponse = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer(msg.getBytes("US-ASCII")));
            nullResponse.setStatus(BinaryMemcacheResponseStatus.KEY_ENOENT);
            nullResponse.setOpaque(request.opaque());
            nullResponse.setOpcode(request.opcode());
            nullResponse.setTotalBodyLength(msg.length());
            ctx.writeAndFlush(nullResponse);
            return;
        }

        ByteBuf content = Unpooled.wrappedBuffer(val.getBytes("UTF-8"));
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeZero(8);
        DefaultFullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, Unpooled.wrappedBuffer(new byte[4]), content);
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        response.setOpcode(request.opcode());
        response.setCas(0);
        response.setOpaque(request.opaque());
        response.setTotalBodyLength(4 + content.capacity());
        ctx.writeAndFlush(response);
    }

    private void handleSet(final ChannelHandlerContext ctx, DefaultFullBinaryMemcacheRequest request) throws Exception {
        int valueSize = request.totalBodyLength() - Short.toUnsignedInt(request.keyLength()) - Byte.toUnsignedInt(request.extrasLength());
        if (valueSize > maxValueSize) {
            String msg = "Value too big.  Max Value is " + maxValueSize;
            DefaultFullBinaryMemcacheResponse nullResponse = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer(msg.getBytes("US-ASCII")));
            nullResponse.setStatus(BinaryMemcacheResponseStatus.E2BIG);
            nullResponse.setOpaque(request.opaque());
            nullResponse.setOpcode(request.opcode());
            nullResponse.setTotalBodyLength(msg.length());
            ctx.writeAndFlush(nullResponse);
            return;
        }

        String key = request.key().toString(CharsetUtil.UTF_8);
        String content = request.content().toString(CharsetUtil.UTF_8);
        SQueue sQueue = getQ(key);
        sQueue.set(content);

        DefaultFullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer("".getBytes("US-ASCII")));
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        response.setOpaque(request.opaque());
        response.setOpcode(request.opcode());
        response.setCas(0);
        response.setTotalBodyLength(0);
        ctx.writeAndFlush(response);
    }

    private void stats(final ChannelHandlerContext ctx, BinaryMemcacheRequest request) throws Exception {
        String qInfo = "";
        for (SQueue sQueue : qMap.values()) {
            qInfo += sQueue.toString() + "\r\n";
        }

        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(Unpooled.wrappedBuffer("version".getBytes(CharsetUtil.UTF_8)), null, Unpooled.wrappedBuffer(qInfo.getBytes(CharsetUtil.UTF_8)));
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        response.setOpaque(request.opaque());
        response.setOpcode(request.opcode());
        ctx.write(response);

        response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer("".getBytes("US-ASCII")));
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        response.setOpaque(request.opaque());
        response.setOpcode(request.opcode());
        response.setCas(0);
        response.setTotalBodyLength(0);
        ctx.writeAndFlush(response);

    }

    private SQueue getQ(String name) throws Exception {
        if (qMap.containsKey(name)) {
            return qMap.get(name);
        }

        synchronized (this) {
            if (qMap.containsKey(name)) {
                return qMap.get(name);
            }
            SQueue q = new SQueue(name);
            qMap.putIfAbsent(name, q);
            return q;
        }
    }

    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 200, 30, TimeUnit.SECONDS, new LinkedBlockingQueue(64), new ThreadPoolExecutor.CallerRunsPolicy()) {
        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            String name = t.getName();
            if (!name.contains("queue")) {
                t.setName("queue-" + name);
            }
            super.beforeExecute(t, r);
        }

        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            if (t != null) {
                logger.error(
                        "beExecutorService failure,command=" + r.getClass().getCanonicalName());
            }
        }
    };

}