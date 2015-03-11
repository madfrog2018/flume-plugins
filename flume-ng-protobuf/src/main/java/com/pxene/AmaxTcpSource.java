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
package com.pxene;

import org.apache.commons.io.HexDump;
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
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AmaxTcpSource extends AbstractSource
			implements EventDrivenSource, Configurable {

	 private static final Logger logger = LoggerFactory
	            .getLogger(AmaxTcpSource.class);
	    private int port;
	    private String host = null;
	    private Channel nettyChannel;
	    private Integer eventSize;
	    private Map<String, String> formaterProp;
	    private CounterGroup counterGroup = new CounterGroup();
	    private Boolean keepFields;

	    public class AmaxLogHandler extends SimpleChannelHandler {
	        private AmaxTcpSourceUtils AmaxTcpSourceUtils = new AmaxTcpSourceUtils();
	        public void setEventSize(int eventSize) {
	            AmaxTcpSourceUtils.setEventSize(eventSize);
	        }
	        public void setKeepFields(boolean keepFields) {
	            AmaxTcpSourceUtils.setKeepFields(keepFields);
	        }
	        public void setFormater(Map<String, String> prop) {
	            AmaxTcpSourceUtils.addFormats(prop);
	        }
	        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
	                throws Exception {
	            logger.debug("ChannelHandlerContext is " + ctx.getName());
	            logger.debug("exception is " + e.toString());
	        }

	        @Override
	        public void messageReceived(ChannelHandlerContext ctx,
	                                    MessageEvent mEvent) {
//	        	logger.info("message received is start");
	            ChannelBuffer buffer = (ChannelBuffer) mEvent.getMessage();
                logger.debug("recieve buffer length is " + buffer.readableBytes());
                try {
                    HexDump.dump(buffer.array(), 0, System.out, 0);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//	            byte[] dateTimeBytes = new byte[8];
//	            long dateLong = 0l;
//	            buffer.readBytes(dateTimeBytes, 0, 8);
//	            dateLong = AmaxTcpSourceUtils.byteArrayToLong(dateTimeBytes);
//	            logger.debug("parse the dataLength is " + buffer.readableBytes());
                long dateLong = buffer.readLong();
                logger.debug("recieve buffer length after read long is " + buffer.readableBytes());

                logger.debug("recieve date long is" + dateLong);
//                logger.info("received the message date is " + dateLong);
	            Event e = null;
//                byte[] data = new byte[buffer.readableBytes()];
//                buffer.readBytes(data, 0,buffer.readableBytes());
//                logger.debug("parse the dataLength is " + buffer.readableBytes());
                ByteBuffer byteBuffer = ByteBuffer.allocate(buffer.readableBytes());
                buffer.readBytes(byteBuffer);
                try {
                    HexDump.dump(byteBuffer.array(), 0, System.out, 0);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                logger.debug("String is " + new String(byteBuffer.array()));
                e = AmaxTcpSourceUtils.buildMessage(dateLong, byteBuffer.array());
                logger.debug("build message is over");
                if (e != null) {
                    try {
                        getChannelProcessor().processEvent(e);
                        logger.debug("events success");
                        counterGroup.incrementAndGet("events.success");
                    } catch (org.apache.flume.ChannelException ex) {
                        counterGroup.incrementAndGet("events.dropped");
                        logger.error("Error writting to channel, event dropped", ex);
                    }
                }
	        }
	    }

	    @Override
	    public void start() {
	        ChannelFactory factory = new NioServerSocketChannelFactory(
	                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
	        ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
//	        serverBootstrap.setOption("reuseAddress", true);//端口重用
//	        serverBootstrap.setOption("child.tcpNoDelay", true);//无延迟
	        serverBootstrap.setOption("child.receiveBufferSize", 65536);//设置接收缓冲区大小
	        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
	            @Override
	            public ChannelPipeline getPipeline() {
	                ChannelPipeline pipeline = new DefaultChannelPipeline();
	                AmaxLogHandler handler = new AmaxLogHandler();
	                handler.setEventSize(eventSize);
	                handler.setFormater(formaterProp);
	                handler.setKeepFields(keepFields);
	                pipeline.addLast("FrameDecoder", new AmaxFrameDecoder());
	                pipeline.addLast("handler", handler);
	                return pipeline;
	            }
	        });
//	        serverBootstrap.setOption("child.bufferFactory",
//	                new HeapChannelBufferFactory(USE_LITTLE_ENDIAN" ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN)
//	        );
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
	        eventSize = context.getInteger("eventSize", AmaxTcpSourceUtils.DEFAULT_SIZE);
	        formaterProp = context.getSubProperties(
	                SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
	        keepFields = context.getBoolean
	                (SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS, false);
	    }
}
