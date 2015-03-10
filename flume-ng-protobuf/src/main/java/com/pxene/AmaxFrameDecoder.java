package com.pxene;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by young on 2015/1/16.
 */
public class AmaxFrameDecoder extends FrameDecoder {
    private final static Logger logger = LoggerFactory.getLogger(AmaxFrameDecoder.class);
    private final static int DATA_LENGTH = 4;
//    private byte[] dataBytes = new byte[DATA_LENGTH];
    private final static int TIME_LENGTH = 8;
//    private TanxTcpSourceUtils ProtobufSourceUtils = new TanxTcpSourceUtils();
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < DATA_LENGTH) {
        	
        	logger.warn("buffer length is less than 4 bytes");
        	return null;
        }
        buffer.markReaderIndex();
//        buffer.readBytes(dataBytes, 0, DATA_LENGTH);
//        int dataLength = ProtobufSourceUtils.byteArrayToInt(dataBytes);
        int dataLength = buffer.readInt();
        logger.debug("parse dataLength is " + dataLength);
//        buffer.resetReaderIndex();
        int nextBytesLength = TIME_LENGTH + dataLength;
        if (buffer.readableBytes() < nextBytesLength) {
//        	logger.warn("buffer length is less than needed bytes");
            buffer.resetReaderIndex();
            return null;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(nextBytesLength);
        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(byteBuffer);
        buffer.readBytes(nextBytesLength);

//        buffer.skipBytes(DATA_LENGTH);
//        int readerIndex = buffer.readerIndex();
//        ChannelBuffer frame = extractFrame(buffer, readerIndex, nextBytesLength);
//        buffer.readerIndex(readerIndex +nextBytesLength);
        
//        logger.info("read over");
        return channelBuffer;
    }
    
//    private ChannelBuffer extractFrame(ChannelBuffer buffer, int index, int length) {
//        ChannelBuffer frame = buffer.factory().getBuffer(length);
//        frame.writeBytes(buffer, index, length);
//        return frame;
//    }
}
