package com.pxene.inmobi;


import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InmobiFrameDecoder extends FrameDecoder {
	
	private final static Logger logger = LoggerFactory.getLogger(InmobiFrameDecoder.class);
	
	private final static int DATA_LENGTH = 4;
	private final static int TIME_LENGTH = 8;
	
	@Override
	protected Object decodeLast(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		// TODO Auto-generated method stub
		return super.decodeLast(ctx, channel, buffer);
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {

		if(buffer.readableBytes() < DATA_LENGTH){
			logger.debug("buffer length is less than 4 bytes");
        	return null;
		}
		
		buffer.markReaderIndex();
		
		int dataLength = buffer.readInt();
		
		int nextBytesLength = TIME_LENGTH + dataLength;
		if(buffer.readableBytes() < nextBytesLength){
			buffer.resetReaderIndex();
			return null;
		}
		ByteBuffer byteBuffer = ByteBuffer.allocate(nextBytesLength);
		ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(byteBuffer);
		buffer.readBytes(byteBuffer);
		
		return channelBuffer;
		
	}

}
