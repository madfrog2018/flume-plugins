/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pxene.protobuf;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufSource extends AbstractSource
        implements EventDrivenSource, Configurable {


    private static final Logger logger = LoggerFactory
            .getLogger(ProtobufSource.class);
    private int port;
    private String host = null;
    private Channel nettyChannel;
    private Integer eventSize;
    private Map<String, String> formaterProp;
    private CounterGroup counterGroup = new CounterGroup();
    private Boolean keepFields;

    public class protobufHandler extends SimpleChannelHandler {

        private ProtobufSourceUtils ProtobufSourceUtils = new ProtobufSourceUtils();

        public void setEventSize(int eventSize){
            ProtobufSourceUtils.setEventSize(eventSize);
        }

        public void setKeepFields(boolean keepFields){
            ProtobufSourceUtils.setKeepFields(keepFields);
        }

        public void setFormater(Map<String, String> prop) {
            ProtobufSourceUtils.addFormats(prop);
        }

        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        	
        	logger.debug("ChannelHandlerContext is " + ctx.getName());
        	logger.debug("exception is " + e.toString());
        }
        
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
            TanxBidding.BidRequest req = (TanxBidding.BidRequest) mEvent.getMessage();
//        	TanxBidding.BidRequest.parseFrom(mEvent.getMessage())
            String bidId = req.getBid();
            logger.debug("update!!!!!!!!!!!!!!!!!");
            logger.debug("bidId is --------------------- " + bidId);
            logger.debug(mEvent.getMessage().toString());

//            while (buff.readable()) {
//                Event e = ProtobufSourceUtils.extractEvent(buff);
//                if (e == null) {
//                    logger.debug("Parsed partial event, event will be generated when " +
//                            "rest of the event is received.");
//                    continue;
//                }
//                try {
//                    getChannelProcessor().processEvent(e);
//                    counterGroup.incrementAndGet("events.success");
//                } catch (ChannelException ex) {
//                    counterGroup.incrementAndGet("events.dropped");
//                    logger.error("Error writting to channel, event dropped", ex);
//                }
//            }

        }
    }

    @Override
    public void start() {
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                protobufHandler handler = new protobufHandler();
//                handler.setEventSize(eventSize);
//                handler.setFormater(formaterProp);
//                handler.setKeepFields(keepFields);
                //set protobuf decoder
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("frameDecoder",
                        new ProtobufVarint32FrameDecoder());
                pipeline.addLast("protobufDecoder", new ProtobufDecoder(TanxBidding.BidRequest.getDefaultInstance()));
                pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
                pipeline.addLast("protobufEncoder", new ProtobufEncoder());
                pipeline.addLast("handler", handler);
                return pipeline;
//                return Channels.pipeline(handler);
            }
        });

        logger.info("Syslog TCP Source starting...");

        if (host == null) {
            nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
        } else {
            nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
        }

        super.start();
    }

    @Override
    public void stop() {
        logger.info("Syslog TCP Source stopping...");
        logger.info("Metrics:{}", counterGroup);

        if (nettyChannel != null) {
            nettyChannel.close();
            try {
                nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("netty server stop interrupted", e);
            } finally {
                nettyChannel = null;
            }
        }

        super.stop();
    }

    @Override
    public void configure(Context context) {
        Configurables.ensureRequiredNonNull(context,
                SyslogSourceConfigurationConstants.CONFIG_PORT);
        port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
        host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
        eventSize = context.getInteger("eventSize", ProtobufSourceUtils.DEFAULT_SIZE);
        formaterProp = context.getSubProperties(
                SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
        keepFields = context.getBoolean
                (SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS, false);
    }

    @VisibleForTesting
    public int getSourcePort() {
        SocketAddress localAddress = nettyChannel.getLocalAddress();
        if (localAddress instanceof InetSocketAddress) {
            InetSocketAddress addr = (InetSocketAddress) localAddress;
            return addr.getPort();
        }
        return 0;
    }

}
