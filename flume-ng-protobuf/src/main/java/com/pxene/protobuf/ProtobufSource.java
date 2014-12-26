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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.HexDump;
import org.apache.flume.ChannelException;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
		
//		@Override
//		public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
//			System.out.println("connected : " + e.getChannel()); 
//			NioSocketChannelConfig config =  (NioSocketChannelConfig) e.getChannel().getConfig(); 
//			config.setBufferFactory(new DirectChannelBufferFactory()); 
//			config.setReceiveBufferSizePredictor(new FixedReceiveBufferSizePredictor(1024*1000));
//		}
		@Override
		public void messageReceived(ChannelHandlerContext ctx,
				MessageEvent mEvent) {
			logger.info("the messageReceived metdo is start");
			ChannelBuffer buffer = (ChannelBuffer) mEvent.getMessage();
			byte[] dateTimeBytes = new byte[8];
			byte[] dataBytes = new byte[4];
			int dataLength = 0;
			int i = 0;
			long dateLong = 0l;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while (buffer.readable()) {
				logger.info("buffer readable");
				Event e = null;
				try {
					if (0 == dataLength && 0l == dateLong) {

						buffer.readBytes(dataBytes, 0, 4);
						try {
							HexDump.dump(dataBytes, 0, System.out,
									0);
						} catch (Exception e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
						dataLength = ProtobufSourceUtils
								.byteArrayToInt(dataBytes);

                        buffer.readBytes(dateTimeBytes, 0, 8);
						try {
							HexDump.dump(dateTimeBytes, 0, System.out,
									0);
						} catch (Exception e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
                        dateLong = ProtobufSourceUtils
                                .byteArrayToLong(dateTimeBytes);
						logger.info("the dataLength is " + dataLength);
						continue;
					} else {
						byte b = buffer.readByte();
						baos.write(b);
						i++;
						if (dataLength == i) {
							logger.info("the buildMessage is over");
							try {
								HexDump.dump(baos.toByteArray(), 0, System.out,
										0);
							} catch (Exception e2) {
								// TODO Auto-generated catch block
								e2.printStackTrace();
							}
							e = ProtobufSourceUtils.buildMessage(dateLong,
									baos.toByteArray());
							dataLength = 0;
							dateLong = 0l;
							i = 0;
							baos.reset();
						}

					}
				} catch (InvalidProtocolBufferException e1) {
					logger.error("InvalidProtocolBufferException is "
							+ e1.toString());
					continue;
				}

				if (e == null) {
					continue;
				}
				try {
					getChannelProcessor().processEvent(e);
					logger.info("events success");
					counterGroup.incrementAndGet("events.success");
				} catch (ChannelException ex) {
					counterGroup.incrementAndGet("events.dropped");
					logger.error("Error writting to channel, event dropped", ex);
				}
			}
			try {
				HexDump.dump(baos.toByteArray(), 0, System.out,
						0);
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
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
                pipeline.addLast("handler", handler);
                //set protobuf decoder
//                ChannelPipeline pipeline = Channels.pipeline();
//                pipeline.addLast("frameDecoder",
//                        new ProtobufVarint32FrameDecoder());
//                pipeline.addLast("protobufDecoder", new ProtobufDecoder(TanxBidding.BidRequest.getDefaultInstance()));
//                pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
//                pipeline.addLast("protobufEncoder", new ProtobufEncoder());
//                pipeline.addLast("handler", handler);
//                return pipeline;
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
