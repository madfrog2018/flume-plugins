package com.pxene.inmobi;

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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InmobiTcpSource extends AbstractSource implements
		EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory
			.getLogger(InmobiTcpSource.class);
	private int port;
	private String host = null;
	private Channel nettyChannel;
	private Integer eventSize;
	private Map<String, String> formaterProp;
	private CounterGroup counterGroup = new CounterGroup();
	private Boolean keepFields;

	public class InmobiLogHandler extends SimpleChannelHandler {
		private InmobiTcpSourceUtils inmobiTcpSourceUtils = new InmobiTcpSourceUtils();

		public void setEventSize(int eventSize) {
			inmobiTcpSourceUtils.setEventSize(eventSize);
		}

		public void setKeepFields(boolean keepFields) {
			inmobiTcpSourceUtils.setKeepFields(keepFields);
		}

		public void setFormater(Map<String, String> prop) {
			inmobiTcpSourceUtils.addFormats(prop);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			e.getCause().printStackTrace();
			logger.debug("exception is " + e.getCause().getMessage());
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx,
				MessageEvent mEvent) throws Exception {
			
			ChannelBuffer buffer = (ChannelBuffer) mEvent.getMessage();
			logger.debug("recieve buffer length is " + buffer.readableBytes());

			long dateLong = buffer.readLong();
			logger.debug("recieve buffer length after read long is "
					+ buffer.readableBytes());
			logger.debug("recieve date long is" + dateLong);

			Event e = null;
			ByteBuffer byteBuffer = ByteBuffer.allocate(buffer.readableBytes());
			buffer.readBytes(byteBuffer);

			logger.debug("String is " + new String(byteBuffer.array()));

			e = InmobiTcpSourceUtils.parseMessage(dateLong, byteBuffer.array());
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
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
		// serverBootstrap.setOption("reuseAddress", true);//端口重用
		// serverBootstrap.setOption("child.tcpNoDelay", true);//无延迟
		serverBootstrap.setOption("child.receiveBufferSize", 65536);// 设置接收缓冲区大小
		serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = new DefaultChannelPipeline();
				InmobiLogHandler handler = new InmobiLogHandler();
				handler.setEventSize(eventSize);
				handler.setFormater(formaterProp);
				handler.setKeepFields(keepFields);
				pipeline.addLast("FrameDecoder", new InmobiFrameDecoder());
				pipeline.addLast("handler", handler);
				return pipeline;
			}
		});
		logger.info("Syslog TCP Source starting...");

		if (host == null) {
			nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
		} else {
			nettyChannel = serverBootstrap.bind(new InetSocketAddress(host,
					port));
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
		port = context
				.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
		host = context
				.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
		eventSize = context.getInteger("eventSize",
				InmobiTcpSourceUtils.DEFAULT_SIZE);
		formaterProp = context
				.getSubProperties(SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
		keepFields = context.getBoolean(
				SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS, false);

	}

}
