package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

    public static void main(String[] args) {

        try {
            @SuppressWarnings("resource")
			Socket socket = new Socket("10.0.2.11", 5140);

//            TanxBidding.BidRequest.Builder builder = TanxBidding.BidRequest.newBuilder();
//            TanxBidding.BidRequest.AdzInfo.Builder AdzInfoOrBuilder = TanxBidding.BidRequest.AdzInfo.newBuilder();
//            TanxBidding.BidRequest.UserAttribute.Builder userBuilder = TanxBidding.BidRequest.UserAttribute.newBuilder();
//            TanxBidding.BidRequest.ContentCategory.Builder contentBuilder = TanxBidding.BidRequest.ContentCategory.newBuilder();
//
//            builder.setBid("0a67f524000054813cfc6d8e0015ad64");
//            builder.setVersion(3);
//            builder.setAdzinfo(1, AdzInfoOrBuilder.setId(1));
//            builder.setAdzinfo(1,AdzInfoOrBuilder.setPid("mm_45015339_4154953_22394227"));
//            builder.setUserAttribute(1, userBuilder.setId(1));
//            builder.setContentCategories(1, contentBuilder.setId(1));
//            builder.setContentCategories(1, contentBuilder.setConfidenceLevel(12));
//            TanxBidding.BidRequest request = builder.build();
//            byte[] result = request.toByteArray();
            
            File file = new File("D:\\git\\flume-ng-protobuf\\src\\main\\resources\\tess.txt");
            @SuppressWarnings("resource")
			FileInputStream inputStream = new FileInputStream(file);
            byte[] result = new byte[inputStream.available()];
            inputStream.read(result);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            out.write(result);
            int totalBytesRcvd = 0;
            int bytesRcvd = 0;
            while (totalBytesRcvd < result.length) {
                if ((bytesRcvd = in.read(result, totalBytesRcvd, result.length - totalBytesRcvd)) == -1) {
                    throw new SocketException("与服务器的连接已关闭");
                }
                totalBytesRcvd += bytesRcvd;
            }
            socket.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
