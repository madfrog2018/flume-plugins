package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

import org.apache.commons.io.HexDump;

import com.pxene.protobuf.TanxBidding.BidRequest.AdzInfo;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

    public static void main(String[] args) {

        try {
            @SuppressWarnings("resource")
			Socket socket = new Socket("10.0.2.11", 5140);

            TanxBidding.BidRequest.Builder builder = TanxBidding.BidRequest.newBuilder();
            TanxBidding.BidRequest.AdzInfo.Builder AdzInfoOrBuilder = TanxBidding.BidRequest.AdzInfo.newBuilder();
            TanxBidding.BidRequest.UserAttribute.Builder userBuilder = TanxBidding.BidRequest.UserAttribute.newBuilder();
            TanxBidding.BidRequest.ContentCategory.Builder contentBuilder = TanxBidding.BidRequest.ContentCategory.newBuilder();

            builder.setBid("0a67f524000054813cfc6d8e0015ad64");
            
            builder.setVersion(3);

            AdzInfoOrBuilder.setId(1);
            AdzInfoOrBuilder.setPid("mm_45015339_4154953_22394227");
            userBuilder.setId(1);
            contentBuilder.setId(1);
            contentBuilder.setConfidenceLevel(12);
            AdzInfoOrBuilder.build();
            userBuilder.build();
            contentBuilder.build();
            builder.addAdzinfo(AdzInfoOrBuilder.build());
            builder.addUserAttribute(userBuilder.build());
            builder.addContentCategories(contentBuilder.build());
            TanxBidding.BidRequest request = builder.build();
//            byte[] result = request.toByteArray();
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(request.toByteArray());
            System.out.println("bid is " + req.getBid());
            List<AdzInfo> ad = req.getAdzinfoList();
            System.out.println("pid is " + req.getAdzinfoList());
            HexDump.dump(request.toByteArray(), 0, System.out, 0);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
	        out.write(request.toByteArray());
	        int totalBytesRcvd = 0;
	        int bytesRcvd = 0;
	        while (totalBytesRcvd < request.toByteArray().length) {
	            if ((bytesRcvd = in.read(request.toByteArray(), totalBytesRcvd, request.toByteArray().length - totalBytesRcvd)) == -1) {
	                throw new SocketException("与服务器的连接已关闭");
	            }
	            totalBytesRcvd += bytesRcvd;
	        }
	        socket.close();
	        System.exit(0);
//            File file = new File("D:\\git\\flume-ng-protobuf\\src\\main\\resources\\tess.txt");
//            @SuppressWarnings("resource")
//			FileInputStream inputStream = new FileInputStream(file);
//            byte[] result = new byte[inputStream.available()];
//            inputStream.read(result);
//            InputStream in = socket.getInputStream();
//            OutputStream out = socket.getOutputStream();
//            out.write(result);
//            int totalBytesRcvd = 0;
//            int bytesRcvd = 0;
//            while (totalBytesRcvd < result.length) {
//                if ((bytesRcvd = in.read(result, totalBytesRcvd, result.length - totalBytesRcvd)) == -1) {
//                    throw new SocketException("与服务器的连接已关闭");
//                }
//                totalBytesRcvd += bytesRcvd;
//            }
//            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
