package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.io.HexDump;
import org.apache.log4j.Logger;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.UnmodifiableLazyStringList;
import com.pxene.protobuf.TanxBidding.BidRequest.AdzInfo;
import com.pxene.protobuf.TanxBidding.BidRequest.ContentCategory;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile.Device;
import com.pxene.protobuf.TanxBidding.BidRequest.UserAttribute;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

	public static final Logger logger = Logger.getLogger(SocketData.class);
	
	public static void main(String[] args) {
		System.out.println(new Date());
        try {
        	for (int i = 0; i < 10000; i++) {
            Socket socket = new Socket("192.168.2.7", 5140);
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
            TanxBidding.BidRequest req1 = TanxBidding.BidRequest.parseFrom(request.toByteArray());
            System.out.println("bid is " + req1.getBid());
            List<AdzInfo> ad = req1.getAdzinfoList();
            System.out.println("pid is " + req1.getAdzinfoList());
            HexDump.dump(request.toByteArray(), 0, System.out, 0);
            OutputStream out = socket.getOutputStream();
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
            
            String spacers = "|";
            Character charSpacers = new Character((char) 0x01);
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
            
            List<Integer> userVertical = req.getUserVerticalList();
            if (userVertical.isEmpty()) {
            	sBuilder.append(NULL).append(spacers);
    		} else {
    			for (Integer integer : userVertical) {
    				sBuilder.append(integer).append(charSpacers);
    			}
    			getSubString(sBuilder).append(spacers);
    		}
            
            if (req.hasTidVersion()) {
            	sBuilder.append(req.getTidVersion()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            
            sBuilder.append(req.getPrivateInfoCount()).append("|");
            
            if (req.hasHostedMatchData()) {
				sBuilder.append(req.getHostedMatchData()).append("|");
			
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            sBuilder.append(req.getUserAttributeCount()).append("|");
            
            List<UserAttribute> userAttributeList = req.getUserAttributeList();
            if (userAttributeList.isEmpty()) {
    			
            	sBuilder.append(NULL).append(spacers);
            	sBuilder.append(NULL).append(spacers);
    		} else {
    			
    			StringBuilder userAtrributeIdBuilder = new StringBuilder();
    			StringBuilder userAtrributeTimestampBuilder = new StringBuilder();
    			for (UserAttribute userAttribute : userAttributeList) {
    				
    				userAtrributeIdBuilder.append(userAttribute.getId()).append(charSpacers);
    				userAtrributeTimestampBuilder.append(userAttribute.getTimestamp()).append(charSpacers);
    			}
    			
    			sBuilder.append(getSubString(userAtrributeIdBuilder).toString()).append(spacers);
    			sBuilder.append(getSubString(userAtrributeTimestampBuilder).toString()).append(spacers);
    			
    		}
            
            ProtocolStringList excludedUrls = req.getExcludedClickThroughUrlList();
            if (excludedUrls.isEmpty()) {
            	sBuilder.append(NULL).append("|");
			} else {
				StringBuilder excludedUrlsBuilder = new StringBuilder();
				for (String string : excludedUrls) {
					excludedUrlsBuilder.append(string).append(new Character((char)0x01));
				}
				sBuilder.append(getSubString(excludedUrlsBuilder)).append("|");
			}
            
            if (req.hasUrl()) {
				
            	sBuilder.append(req.getUrl()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasCategory()) {
				sBuilder.append(req.getCategory()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasAdxType()) {
				sBuilder.append(req.getAdxType()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasAnonymousId()) {
            	sBuilder.append(req.getAnonymousId()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasDetectedLanguage()) {
            	sBuilder.append(req.getDetectedLanguage()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (req.hasCategoryVersion()) {
				sBuilder.append(req.getCategoryVersion()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            List<AdzInfo> adzInfos = req.getAdzinfoList();
            StringBuilder adzInfoIdBuilder = new StringBuilder();
            StringBuilder adzInfoPidBuilder = new  StringBuilder();
            StringBuilder adzInfoSizeBuilder = new StringBuilder();
            StringBuilder adzInfoAdBidCountBuilder = new StringBuilder();
            StringBuilder adzInfoViewTypeBuilder = new StringBuilder();
            StringBuilder adzInfoExcludedFilterBuilder = new StringBuilder();
            StringBuilder adzInfoMinCPMPriceBuilder = new StringBuilder();
            StringBuilder adzInfoViewScreenBuilder = new StringBuilder();
            StringBuilder adzInfoPageSessionAdIdxbBuilder = new StringBuilder();
            for (AdzInfo adzInfo : adzInfos) {
				
            	if (adzInfo.hasId()) {
					adzInfoIdBuilder.append(adzInfo.getId()).append(new Character((char)0x01));
				}
            	if (adzInfo.hasPid()) {
					adzInfoPidBuilder.append(adzInfo.getPid()).append(new Character((char)0x01));
				}
            	if (adzInfo.hasSize()) {
					adzInfoSizeBuilder.append(adzInfo.getSize()).append(new Character((char)0x01));
				}
            	if (adzInfo.hasAdBidCount()) {
					adzInfoAdBidCountBuilder.append(adzInfo.getAdBidCount()).append(new Character((char)0x01));
				}
            	
            	List<Integer> viewTypes = adzInfo.getViewTypeList();
            	for (Integer integer : viewTypes) {
					
            		adzInfoViewTypeBuilder.append(integer).append(new Character((char)0x01));
				}
            	
            	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
            	for (Integer integer1 : excludedFilters) {
					adzInfoExcludedFilterBuilder.append(integer1).append(new Character((char)0x01));
				}
            	
            	if (adzInfo.hasMinCpmPrice()) {
					
            		adzInfoMinCPMPriceBuilder.append(adzInfo.getMinCpmPrice()).append(new Character((char)0x01));
				}
            	
            	
            	if (adzInfo.hasViewScreen()) {
					adzInfoViewScreenBuilder.append(adzInfo.getViewScreen().name()).append(new Character((char)0x01));
				}
            	
            	if (adzInfo.hasPageSessionAdIdx()) {
					adzInfoPageSessionAdIdxbBuilder.append(adzInfo.getPageSessionAdIdx()).append(new Character((char)0x01));
				}
			}
            
            sBuilder.append(getSubString(adzInfoIdBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoPidBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoSizeBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoAdBidCountBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoViewTypeBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoExcludedFilterBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoMinCPMPriceBuilder)).append("|");
            sBuilder.append(getSubString(adzInfoViewScreenBuilder)).append("|");
            if (req.hasPageSessionId()) {
				sBuilder.append(req.getPageSessionId()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            sBuilder.append(getSubString(adzInfoPageSessionAdIdxbBuilder)).append("|");
            
            List<Integer> excludedSensitiveCategorys = req.getExcludedSensitiveCategoryList();
            StringBuilder excludedSensitiveBuilder = new StringBuilder();
            if (excludedSensitiveCategorys.isEmpty()) {
            	sBuilder.append(NULL).append("|");
			} else {
				for (Integer integer : excludedSensitiveCategorys) {
					excludedSensitiveBuilder.append(integer).append(new Character((char)0x01));
				}
				sBuilder.append(getSubString(excludedSensitiveBuilder)).append("|");
			}
            
            List<Integer> excludedAdCategorys = req.getExcludedAdCategoryList();
            StringBuilder excludedAdCategorysBuilder = new StringBuilder();
            if (excludedAdCategorys.isEmpty()) {
            	sBuilder.append(NULL).append("|");
			} else {
				for (Integer integer : excludedAdCategorys) {
					excludedAdCategorysBuilder.append(integer).append(new Character((char)0x01));
				}
				sBuilder.append(getSubString(excludedAdCategorysBuilder)).append("|");
			}
            
            sBuilder.append(req.getContentCategoriesCount()).append("|");
            
            List<ContentCategory> contentCategories = req.getContentCategoriesList();
            StringBuilder contentCategoryIdBuilder = new StringBuilder();
            StringBuilder contentCategoryConfidenceLevelBuilder = new StringBuilder();
            if (contentCategories.isEmpty()) {
				sBuilder.append(NULL).append("|");
				sBuilder.append(NULL).append("|");
			} else {
				for (ContentCategory contentCategory : contentCategories) {
	            	
	            	if (contentCategory.hasId()) {
						contentCategoryIdBuilder.append(contentCategory.getId()).append(new Character((char)0x01));
					} else {
						contentCategoryIdBuilder.append(NULL).append(new Character((char)0x01));
					}
	            	if (contentCategory.hasConfidenceLevel()) {
						contentCategoryConfidenceLevelBuilder.append(contentCategory.getConfidenceLevel()).append(new Character((char)0x01));
					} else {
						contentCategoryIdBuilder.append(NULL).append(new Character((char)0x01));
					}
				}
				sBuilder.append(getSubString(contentCategoryIdBuilder)).append("|");
				sBuilder.append(getSubString(contentCategoryConfidenceLevelBuilder)).append("|");
			}
            
            //获取移动设备的信息
            Mobile mobile = req.getMobile();
            if (mobile.hasIsApp()) {
				sBuilder.append(mobile.getIsApp()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (mobile.hasAdNum()) {
				sBuilder.append(mobile.getAdNum()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            ProtocolStringList mobileAdKeywords = mobile.getAdKeywordList();
            if (mobileAdKeywords.isEmpty()) {
				sBuilder.append(NULL).append("|");
			} else {
				StringBuilder adKeywordsBuilder = new StringBuilder();
				for (String string : mobileAdKeywords) {
					adKeywordsBuilder.append(string).append(new Character((char)0x01));
				}
				sBuilder.append(getSubString(adKeywordsBuilder)).append("|");
			}
            
            if (mobile.hasPackageName()) {
				sBuilder.append(mobile.getPackageName()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            Device device = mobile.getDevice();
            if (device.hasPlatform()) {
				sBuilder.append(device.getPlatform()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasBrand()) {
				sBuilder.append(device.getBrand()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasModel()) {
				sBuilder.append(device.getModel()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasOs()) {
				sBuilder.append(device.getOs()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasOsVersion()) {
				sBuilder.append(device.getOsVersion()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
           
            if (device.hasNetwork()) {
				sBuilder.append(device.getNetwork()).append("|"); 
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasOperator()) {
				sBuilder.append(device.getOperator()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasLongitude()) {
				sBuilder.append(device.getLongitude()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasLatitude()) {
				sBuilder.append(device.getLatitude()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasDeviceSize()) {
            	sBuilder.append(device.getDeviceSize()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasDevicePixelRatio()) {
				sBuilder.append(device.getDevicePixelRatio()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
            
            if (device.hasDeviceId()) {
				sBuilder.append(device.getDeviceId()).append("|");
			} else {
				sBuilder.append(NULL).append("|");
			}
           
            System.out.println(sBuilder.toString());
            System.out.println(sBuilder.toString().getBytes());
            HexDump.dump(sBuilder.toString().getBytes(), 0, System.out, 0);
            Map<String, String> map = new HashMap<String, String>();
            Map<FieldDescriptor, Object> fields = req.getAllFields();
            Set<FieldDescriptor> fieldDescriptors = fields.keySet();
            for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            	if (fieldDescriptor.getJavaType() == JavaType.INT) {
            		if (fields.get(fieldDescriptor) instanceof Integer) {
            			map.put(fieldDescriptor.getName(), String.valueOf(fields.get(fieldDescriptor)));
					} else if (fields.get(fieldDescriptor) instanceof UnmodifiableList) {
						List<Integer> integers = (List<Integer>) fields.get(fieldDescriptor);
						StringBuilder integerStringBuilder = new StringBuilder();
						for (Integer integer : integers) {
							integerStringBuilder.append(String.valueOf(integer));
							integerStringBuilder.append(new Character((char) 0x01));
						}
					} else {
						logger.info("is JavaType.INT not Integer");
					}
				}else if (fieldDescriptor.getJavaType() == JavaType.STRING) {
					if (fields.get(fieldDescriptor) instanceof UnmodifiableLazyStringList) {
						
						List<String> strings = (List<String>) fields.get(fieldDescriptor);
						StringBuilder fieldStringBuilder = new StringBuilder();
						for (String string : strings) {
							fieldStringBuilder.append(string);
							fieldStringBuilder.append(new Character((char) 0x01));
						}
						map.put(fieldDescriptor.getName(), fieldStringBuilder.toString());
					} else {
						String fieldValue = (String)fields.get(fieldDescriptor);
						map.put(fieldDescriptor.getName(), fieldValue.substring(0, fieldValue.length()-2));
					}
					
				}
					
			}

//			}
//            OutputStream out1 = socket.getOutputStream();
            
            	out.write(result);
            	socket.close();
			}
//            out.write(result);
        	System.out.println(new Date());

        } catch (IOException e) {
            logger.error("error is " + e.toString());
        }
    }
    
    public static StringBuilder getSubString(StringBuilder sb){
    	
    	if (sb == null) {
			return null;
		}
    	
    	String subString = sb.toString().substring(0, sb.length()-1);
    	sb.delete(0, sb.length()).append(subString);
    	return sb;
    }
}
