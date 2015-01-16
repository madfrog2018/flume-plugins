package com.pxene.protobuf;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * Created by young on 2015/1/16.
 */
public class ProtobufFrameDecoder extends FrameDecoder {

    private final static Logger logger = LoggerFactory.getLogger(ProtobufFrameDecoder.class);
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer) throws Exception {
        ProtobufSourceUtils protobufSourceUtils = new ProtobufSourceUtils();
        byte[] dataBytes = new byte[4];
        int dataLength = 0;
        int i = 0;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (buffer.readable()) {

            if (dataLength == 0) {

                buffer.readBytes(dataBytes, 0, 4);
                dataLength = protobufSourceUtils.byteArrayToInt(dataBytes);
                logger.info("datalength in frame is " + dataLength);
            } else {

                byte b = buffer.readByte();
                baos.write(b);
                i++;
                if (dataLength == i) {
                    logger.info("the buildMessage is over");
                    dataLength = 0;
                    i = 0;
                    baos.reset();
                }
            }



        }


        return null;
    }
}
