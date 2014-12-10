package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.HexDump;
import org.apache.log4j.Logger;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.pxene.protobuf.TanxBidding.BidRequest.AdzInfo;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

	public static final Logger logger = Logger.getLogger(SocketData.class);
    public static void main(String[] args) {

        try {
        	
//            Socket socket = new Socket("10.0.2.11", 5140);
//            TanxBidding.BidRequest.Builder builder = TanxBidding.BidRequest.newBuilder();
//            TanxBidding.BidRequest.AdzInfo.Builder AdzInfoOrBuilder = TanxBidding.BidRequest.AdzInfo.newBuilder();
//            TanxBidding.BidRequest.UserAttribute.Builder userBuilder = TanxBidding.BidRequest.UserAttribute.newBuilder();
//            TanxBidding.BidRequest.ContentCategory.Builder contentBuilder = TanxBidding.BidRequest.ContentCategory.newBuilder();
//            builder.setBid("0a67f524000054813cfc6d8e0015ad64");
//            builder.setVersion(3);
//            AdzInfoOrBuilder.setId(1);
//            AdzInfoOrBuilder.setPid("mm_45015339_4154953_22394227");
//            userBuilder.setId(1);
//            contentBuilder.setId(1);
//            contentBuilder.setConfidenceLevel(12);
//            AdzInfoOrBuilder.build();
//            userBuilder.build();
//            contentBuilder.build();
//            builder.addAdzinfo(AdzInfoOrBuilder.build());
//            builder.addUserAttribute(userBuilder.build());
//            builder.addContentCategories(contentBuilder.build());
//            TanxBidding.BidRequest request = builder.build();
//            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(request.toByteArray());
//            System.out.println("bid is " + req.getBid());
//            List<AdzInfo> ad = req.getAdzinfoList();
//            System.out.println("pid is " + req.getAdzinfoList());
//            HexDump.dump(request.toByteArray(), 0, System.out, 0);
//            OutputStream out = socket.getOutputStream();
//	        out.write(request.toByteArray());
            File file = new File("D:\\git\\flume-plugins\\flume-ng-protobuf\\src\\main\\resources\\tess.txt");
            @SuppressWarnings("resource")
			FileInputStream inputStream = new FileInputStream(file);
            byte[] result = new byte[inputStream.available()];
            inputStream.read(result);
            System.out.println(result);
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(result);
            Map<FieldDescriptor, Object> fields = req.getAllFields();
            Set<FieldDescriptor> maps = fields.keySet();
            for (FieldDescriptor fieldDescriptor : maps) {
				
            	String name = fieldDescriptor.getName();
            	Object value = fields.get(name);
            	logger.info("value is " + (String)value);
            	if (value instanceof AdzInfo) {
					
            		AdzInfo adzInfo = (AdzInfo) value;
            		Map<FieldDescriptor, Object> adzInfoFileds = adzInfo.getAllFields();
            		Set<FieldDescriptor> adzInfoKeys = adzInfoFileds.keySet();
            		for (FieldDescriptor key : adzInfoKeys) {
						
            			String keyName = key.getName();
            			String keyValue = (String) adzInfoFileds.get(keyName);
					}
            		
            		
				}
            	System.out.println("name is " + name);
			}
//            OutputStream out1 = socket.getOutputStream();
//            out.write(result);
//            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
