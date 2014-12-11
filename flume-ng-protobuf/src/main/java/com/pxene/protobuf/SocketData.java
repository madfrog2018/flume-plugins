package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.HexDump;
import org.apache.log4j.Logger;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.UnmodifiableLazyStringList;
import com.pxene.protobuf.TanxBidding.BidRequest;
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
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(result);
            List<Map<String, String>> list = new ArrayList<Map<String,String>>();
            Map<FieldDescriptor, Object> fields = req.getAllFields();
            Set<FieldDescriptor> fieldDescriptors = fields.keySet();
            for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            	Map<String, String> map = new HashMap<String, String>();
//            	fieldDescriptor.getJavaType() == JavaType.
            	if (fieldDescriptor.getJavaType() == JavaType.INT) {
            		if (fields.get(fieldDescriptor) instanceof Integer) {
            			map.put(fieldDescriptor.getName(), String.valueOf(fields.get(fieldDescriptor)));
					} else {
						logger.info("is JavaType.INT not Integer");
					}
					list.add(map);
				}else if (fieldDescriptor.getJavaType() == JavaType.STRING) {
					if (fields.get(fieldDescriptor) instanceof UnmodifiableLazyStringList) {
						@SuppressWarnings("unchecked")
						List<String> strings = (List<String>) fields.get(fieldDescriptor);
						StringBuilder fieldStringBuilder = new StringBuilder();
						for (String string : strings) {
							fieldStringBuilder.append(string);
							fieldStringBuilder.append(new Character((char) 0x01));
						}
						map.put(fieldDescriptor.getName(), fieldStringBuilder.toString());
					} else {
						map.put(fieldDescriptor.getName(), (String)fields.get(fieldDescriptor));
					}
					list.add(map);
				}else if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
//					logger.info(fieldDescriptor.toProto().getRepeatedFieldCount(fieldDescriptor));
//					Object subClass = fields.get(fieldDescriptor);
//					if (subClass instanceof AdzInfo) {
//						
//						AdzInfo adzInfo = (AdzInfo) subClass;
//						adzInfoFields = adzInfo.getAllFields();
						
					}
					
				}

//			}
//            OutputStream out1 = socket.getOutputStream();
//            out.write(result);
//            socket.close();

        } catch (IOException e) {
            logger.error("error is " + e.toString());
        }
    }
}
