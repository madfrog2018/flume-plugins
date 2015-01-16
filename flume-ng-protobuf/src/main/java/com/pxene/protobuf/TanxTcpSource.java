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

import com.google.protobuf.InvalidProtocolBufferException;
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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by young on 2015/1/15.
 */
public class TanxTcpSource extends AbstractSource
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

    public class ProtobufHandler extends SimpleChannelHandler {
        private ProtobufSourceUtils ProtobufSourceUtils = new ProtobufSourceUtils();
        public void setEventSize(int eventSize) {
            ProtobufSourceUtils.setEventSize(eventSize);
        }
        public void setKeepFields(boolean keepFields) {
            ProtobufSourceUtils.setKeepFields(keepFields);
        }
        public void setFormater(Map<String, String> prop) {
            ProtobufSourceUtils.addFormats(prop);
        }
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
                throws Exception {
            logger.debug("ChannelHandlerContext is " + ctx.getName());
            logger.debug("exception is " + e.toString());
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx,
                                    MessageEvent mEvent) {
        	logger.info("message received is start");
            ChannelBuffer buffer = (ChannelBuffer) mEvent.getMessage();
            logger.info("channel buffer length is " + buffer.array().length);
            byte[] dateTimeBytes = new byte[8];
            byte[] dataBytes = new byte[4];
            int dataLength = 0;
            long dateLong = 0l;
            
            while (buffer.readable()) {
                Event e = null;
                try {
                    if (0 == dataLength && 0l == dateLong) {

                        buffer.readBytes(dataBytes, 0, 4);
//						try {
//							HexDump.dump(dataBytes, 0, System.out,
//									0);
//						} catch (Exception e2) {
//							// TODO Auto-generated catch block
//							e2.printStackTrace();
//						}
                        dataLength = ProtobufSourceUtils
                                .byteArrayToInt(dataBytes);

                        buffer.readBytes(dateTimeBytes, 0, 8);
//						try {
//							HexDump.dump(dateTimeBytes, 0, System.out,
//									0);
//						} catch (Exception e2) {
//							// TODO Auto-generated catch block
//							e2.printStackTrace();
//						}
                        dateLong = ProtobufSourceUtils
                                .byteArrayToLong(dateTimeBytes);
                        logger.info("the dataLength is " + dataLength);
                        continue;
                    } else {
//                        byte b = buffer.readByte();
//                        baos.write(b);
//                        i++;
//                        if (dataLength == i) {
//                            logger.info("the buildMessage is over");
//
//                            e = ProtobufSourceUtils.buildMessage(dateLong,
//                                    baos.toByteArray());
//                            dataLength = 0;
//                            dateLong = 0l;
//                            i = 0;
//                            baos.reset();
//                        }
                        byte[] data = new byte[dataLength];
                        buffer.readBytes(data, 0,dataLength);
                        e = ProtobufSourceUtils.buildMessage(dateLong, data);
                        if (e == null) {
                            continue;
                        }
                        try {
                            getChannelProcessor().processEvent(e);
                            logger.info("events success");
                            counterGroup.incrementAndGet("events.success");
                        } catch (org.apache.flume.ChannelException ex) {
                            counterGroup.incrementAndGet("events.dropped");
                            logger.error("Error writting to channel, event dropped", ex);
                        }
                        dataLength = 0;
                        dateLong = 0l;
                        logger.info("build message is over");
                    }
                    
                    
                } catch (InvalidProtocolBufferException e1) {
                    logger.error("InvalidProtocolBufferException is "
                            + e1.toString());
                }
                
                logger.info("left byte length is " + buffer.array().length);
            }
        }
    }

    @Override
    public void start() {
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
        serverBootstrap.setOption("reuseAddress", true);//端口重用
        serverBootstrap.setOption("child.tcpNoDelay", true);//无延迟
        serverBootstrap.setOption("child.receiveBufferSize", 2048*1000);//设置接收缓冲区大小
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipeline = new DefaultChannelPipeline();
                ProtobufHandler handler = new ProtobufHandler();
                handler.setEventSize(eventSize);
                handler.setFormater(formaterProp);
                handler.setKeepFields(keepFields);
//                pipeline.addLast("FrameDecode", );
                pipeline.addLast("handler", handler);
                return pipeline;
            }
        });
//        serverBootstrap.setOption("child.bufferFactory",
//                new HeapChannelBufferFactory(USE_LITTLE_ENDIAN" ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN)
//        );
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
}
