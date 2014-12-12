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

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.io.HexDump;
import org.apache.log4j.Logger;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.UnmodifiableLazyStringList;
import com.pxene.protobuf.TanxBidding.BidRequest;
import com.pxene.protobuf.TanxBidding.BidRequest.AdzInfo;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile;
import com.pxene.protobuf.TanxBidding.BidRequest.PrivateInfo;
import com.pxene.protobuf.TanxBidding.BidRequest.UserAttribute;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

	public static final Logger logger = Logger.getLogger(SocketData.class);
	
    @SuppressWarnings("unchecked")
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
//            Descriptor des = BidRequest.getDescriptor();
//            List<FieldDescriptor> fieldLists = des.getFields();
//            for (FieldDescriptor fieldList : fieldLists) {
//				if (fieldList.getJavaType() != JavaType.MESSAGE) {
//					logger.info(fieldList.getName());
//				}
//			}
            
            
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(result);
            StringBuilder sBuilder = new StringBuilder();
            String NULL = "NULL";
            sBuilder.append(req.getVersion()).append("|");
            sBuilder.append(req.getBid()).append("|");
            
            if (req.hasIsTest()) {
            	sBuilder.append(req.getIsTest()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasIsPing()) {
            	sBuilder.append(req.getIsPing()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasTid()) {
            	sBuilder.append(req.getTid()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasIp()) {
				sBuilder.append(req.getIp()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasUserAgent()) {
				
            	sBuilder.append(req.getUserAgent()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasTimezoneOffset()) {
            	
            	sBuilder.append(req.getTimezoneOffset()).append("|");
            } else {
            	sBuilder.append(NULL).append("|");
            }
            
            if (req.hasTimezoneOffset()) {
            	
            	sBuilder.append(req.getTimezoneOffset()).append("|");
            } else {
            	sBuilder.append(NULL).append("|");
            }
            
            List<Integer> userVertical = req.getUserVerticalList();
            if (userVertical.isEmpty()) {
            	sBuilder.append(NULL).append("|");
			} else {
				StringBuilder userVerticalStringBuilder = new StringBuilder();
				for (Integer integer : userVertical) {
					userVerticalStringBuilder.append(integer).append(new Character((char)0x01));
				}
				sBuilder.append(userVerticalStringBuilder.toString().substring(0, 
						userVerticalStringBuilder.toString().length()-2)).append("|");
			}
            
            if (req.hasTidVersion()) {
            	sBuilder.append(req.getTidVersion()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            ProtocolStringList excludedUrls = req.getExcludedClickThroughUrlList();
            if (excludedUrls.isEmpty()) {
            	sBuilder.append(NULL).append("|");
			} else {
				StringBuilder excludedUrlsBuilder = new StringBuilder();
				for (String string : excludedUrls) {
					excludedUrlsBuilder.append(string).append(new Character((char)0x01));
				}
				sBuilder.append(excludedUrlsBuilder.toString().substring(0, 
						excludedUrlsBuilder.toString().length()-2)).append("|");
			}
            
            sBuilder.append(req.getPrivateInfoCount()).append("|");
            
            if (req.hasHostedMatchData()) {
				sBuilder.append(req.getHostedMatchData()).append("|");
			
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            sBuilder.append(req.getUserAttributeCount()).append("|");
            
            
            
            
            
            
//            Map<String, String> map = new HashMap<String, String>();
//            Map<FieldDescriptor, Object> fields = req.getAllFields();
//            Set<FieldDescriptor> fieldDescriptors = fields.keySet();
//            for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
//            	if (fieldDescriptor.getJavaType() == JavaType.INT) {
//            		if (fields.get(fieldDescriptor) instanceof Integer) {
//            			map.put(fieldDescriptor.getName(), String.valueOf(fields.get(fieldDescriptor)));
//					} else if (fields.get(fieldDescriptor) instanceof UnmodifiableList) {
//						List<Integer> integers = (List<Integer>) fields.get(fieldDescriptor);
//						StringBuilder integerStringBuilder = new StringBuilder();
//						for (Integer integer : integers) {
//							integerStringBuilder.append(String.valueOf(integer));
//							integerStringBuilder.append(new Character((char) 0x01));
//						}
//					} else {
//						logger.info("is JavaType.INT not Integer");
//					}
//				}else if (fieldDescriptor.getJavaType() == JavaType.STRING) {
//					if (fields.get(fieldDescriptor) instanceof UnmodifiableLazyStringList) {
//						
//						List<String> strings = (List<String>) fields.get(fieldDescriptor);
//						StringBuilder fieldStringBuilder = new StringBuilder();
//						for (String string : strings) {
//							fieldStringBuilder.append(string);
//							fieldStringBuilder.append(new Character((char) 0x01));
//						}
//						map.put(fieldDescriptor.getName(), fieldStringBuilder.toString());
//					} else {
//						String fieldValue = (String)fields.get(fieldDescriptor);
//						map.put(fieldDescriptor.getName(), fieldValue.substring(0, fieldValue.length()-2));
//					}
//					
//				}
//					
//			}

//			}
//            OutputStream out1 = socket.getOutputStream();
//            out.write(result);
//            socket.close();

        } catch (IOException e) {
            logger.error("error is " + e.toString());
        }
    }
    
}
