package com.pxene.protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
import com.pxene.protobuf.TanxBidding.BidRequest.Video;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.Content;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.VideoFormat;
import com.pxene.protobuf.TanxBidding.BidRequest.VideoOrBuilder;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import com.sun.org.apache.xpath.internal.compiler.Keywords;

/**
 * Created by root
 * 2014/12/5.
 */
public class SocketData {

	public static final Logger logger = Logger.getLogger(SocketData.class);
	
	public static void main(String[] args) {
		System.out.println(new Date());
		int count = 1;
        try {
        	for (int i = 0; i < 5000; i++) {
        	
        		Socket socket = new Socket("192.168.2.7", 5140);
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
//            TanxBidding.BidRequest req1 = TanxBidding.BidRequest.parseFrom(request.toByteArray());
//            System.out.println("bid is " + req1.getBid());
//            List<AdzInfo> ad = req1.getAdzinfoList();
//            System.out.println("pid is " + req1.getAdzinfoList());
//            HexDump.dump(request.toByteArray(), 0, System.out, 0);
            File file = new File("D:\\git\\flume-plugins\\flume-ng-protobuf\\src\\main\\resources\\tess.txt");
            @SuppressWarnings("resource")
			FileInputStream inputStream = new FileInputStream(file);
            byte[] result = new byte[inputStream.available()];
            inputStream.read(result);
            String spacers = "|";
            Character charSpacers = new Character((char)0x01);
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(result);
            StringBuilder sBuilder = new StringBuilder();
            sBuilder.append(req.getVersion()).append(spacers);
            sBuilder.append(req.getBid()).append(spacers);
            
            if (req.hasIsTest()) {
            	sBuilder.append(req.getIsTest()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasIsPing()) {
            	sBuilder.append(req.getIsPing()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasTid()) {
            	sBuilder.append(req.getTid()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasIp()) {
				sBuilder.append(req.getIp()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasUserAgent()) {
				
            	sBuilder.append(req.getUserAgent()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasTimezoneOffset()) {
            	
            	sBuilder.append(req.getTimezoneOffset()).append(spacers);
            } else {
            	sBuilder.append(spacers);
            }
            
            List<Integer> userVertical = req.getUserVerticalList();
            if (userVertical.isEmpty()) {
            	sBuilder.append(spacers);
    		} else {
    			for (Integer integer : userVertical) {
    				sBuilder.append(integer).append(charSpacers);
    			}
    			getSubString(sBuilder).append(spacers);
    		}
            
            if (req.hasTidVersion()) {
            	sBuilder.append(req.getTidVersion()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            
            sBuilder.append(req.getPrivateInfoCount()).append(spacers);
            
            if (req.hasHostedMatchData()) {
				sBuilder.append(req.getHostedMatchData()).append(spacers);
			
			} else {
				sBuilder.append(spacers);
			}
            
            sBuilder.append(req.getUserAttributeCount()).append(spacers);
            
            List<UserAttribute> userAttributeList = req.getUserAttributeList();
            if (userAttributeList.isEmpty()) {
    			
            	sBuilder.append(spacers);
            	sBuilder.append(spacers);
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
            	sBuilder.append(spacers);
			} else {
				
				for (String string : excludedUrls) {
					sBuilder.append(string).append(charSpacers);
				}
				getSubString(sBuilder).append(spacers);
			}
            
            if (req.hasUrl()) {
				
            	sBuilder.append(req.getUrl()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasCategory()) {
				sBuilder.append(req.getCategory()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasAdxType()) {
				sBuilder.append(req.getAdxType()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasAnonymousId()) {
            	sBuilder.append(req.getAnonymousId()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasDetectedLanguage()) {
            	sBuilder.append(req.getDetectedLanguage()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (req.hasCategoryVersion()) {
				sBuilder.append(req.getCategoryVersion()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            List<AdzInfo> adzInfos = req.getAdzinfoList();
            StringBuilder adzInfoViewTypeBuilder = new StringBuilder();
            StringBuilder adzInfoExcludedFilterBuilder = new StringBuilder();
            StringBuilder adzInfoBuilder = new StringBuilder();
            String[] mergeredValues = new String[8];
            int adzInfoNum = 0;
            for (AdzInfo adzInfo : adzInfos) {
				if (adzInfoNum == 0) {
					if (adzInfo.hasId()) {
	            		adzInfoBuilder.append(adzInfo.getId()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	if (adzInfo.hasPid()) {
	            		adzInfoBuilder.append(adzInfo.getPid()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	if (adzInfo.hasSize()) {
	            		adzInfoBuilder.append(adzInfo.getSize()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	if (adzInfo.hasAdBidCount()) {
	            		adzInfoBuilder.append(adzInfo.getAdBidCount()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	
	            	List<Integer> viewTypes = adzInfo.getViewTypeList();
	            	if (viewTypes.isEmpty()) {
						adzInfoBuilder.append(spacers);
					} else {
						for (Integer integer : viewTypes) {
							
		            		adzInfoViewTypeBuilder.append(integer).append(charSpacers);
						}
						adzInfoBuilder.append(getSubString(adzInfoViewTypeBuilder)).append(spacers);
					}
	            	
	            	
	            	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
	            	if (excludedFilters.isEmpty()) {
	            		adzInfoBuilder.append(spacers);
					} else {
						for (Integer integer : excludedFilters) {
							adzInfoExcludedFilterBuilder.append(integer).append(charSpacers);
						}
						adzInfoBuilder.append(getSubString(adzInfoExcludedFilterBuilder)).append(spacers);
					}
	            	
	            	
	            	if (adzInfo.hasMinCpmPrice()) {
						
	            		adzInfoBuilder.append(adzInfo.getMinCpmPrice()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	
	            	
	            	if (adzInfo.hasViewScreen()) {
	            		adzInfoBuilder.append(adzInfo.getViewScreen().name()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
	            	
	            	if (adzInfo.hasPageSessionAdIdx()) {
	            		adzInfoBuilder.append(adzInfo.getPageSessionAdIdx()).append(spacers);
					} else {
						adzInfoBuilder.append(spacers);
					}
				} else {
					
					String[] values = adzInfoBuilder.toString().split("\\|");
					if (adzInfo.hasId()) {
						mergeredValues[0] = values[0] + charSpacers + adzInfo.getId();
					} else {
						mergeredValues[0] = values[0] + charSpacers;
					}
	            	if (adzInfo.hasPid()) {
	            		mergeredValues[1] = values[1] + charSpacers + adzInfo.getPid();
					} else {
						mergeredValues[1] = values[1] + charSpacers;
					}
	            	if (adzInfo.hasSize()) {
	            		mergeredValues[2] = values[2] + charSpacers + adzInfo.getSize();
					} else {
						mergeredValues[2] = values[2] + charSpacers;
					}
	            	if (adzInfo.hasAdBidCount()) {
	            		mergeredValues[3] = values[3] + charSpacers + adzInfo.getAdBidCount();
					} else {
						mergeredValues[3] = values[3] + charSpacers;
					}
	            	
	            	List<Integer> viewTypes = adzInfo.getViewTypeList();
	            	if (viewTypes.isEmpty()) {
	            		mergeredValues[4] = values[4] + charSpacers;
					} else {
						for (Integer integer : viewTypes) {
							
		            		adzInfoViewTypeBuilder.append(integer).append(charSpacers);
						}
						mergeredValues[4] = values[4] + charSpacers + getSubString(adzInfoViewTypeBuilder).toString();
					}
	            	
	            	
	            	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
	            	if (excludedFilters.isEmpty()) {
	            		mergeredValues[5] = values[5] + charSpacers;
					} else {
						for (Integer integer1 : excludedFilters) {
							adzInfoExcludedFilterBuilder.append(integer1).append(charSpacers);
						}
						mergeredValues[5] = values[5] + charSpacers + getSubString(adzInfoExcludedFilterBuilder).toString();
					}
	            	
	            	
	            	if (adzInfo.hasMinCpmPrice()) {
						
	            		mergeredValues[6] = values[6] + charSpacers + adzInfo.getMinCpmPrice();
					} else {
						mergeredValues[6] = values[6] + charSpacers;
					}
	            	
	            	
	            	if (adzInfo.hasViewScreen()) {
	            		mergeredValues[7] = values[7] + charSpacers + adzInfo.getViewScreen().name();
					} else {
						mergeredValues[7] = values[7] + charSpacers;
					}
	            	
	            	if (adzInfo.hasPageSessionAdIdx()) {
	            		mergeredValues[8] = values[8] + charSpacers + adzInfo.getPageSessionAdIdx();
					} else {
						mergeredValues[8] = values[8] + charSpacers;
					}
				}
            	
            	adzInfoNum++;
			}
            
            if (adzInfos.size() > 1) {
                adzInfoBuilder.delete(0, adzInfoBuilder.length());
                for (int j = 0; j < mergeredValues.length -1; j++) {
    				
                	adzInfoBuilder.append(mergeredValues[j]).append(spacers);
    			}
			}
            sBuilder.append(getSubString(adzInfoBuilder)).append(spacers);
            if (req.hasPageSessionId()) {
				sBuilder.append(req.getPageSessionId()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            
            List<Integer> excludedSensitiveCategorys = req.getExcludedSensitiveCategoryList();
            
            if (excludedSensitiveCategorys.isEmpty()) {
            	sBuilder.append(spacers);
			} else {
				for (Integer integer : excludedSensitiveCategorys) {
					sBuilder.append(integer).append(charSpacers);
				}
				sBuilder.append(getSubString(sBuilder)).append(spacers);
			}
            
            List<Integer> excludedAdCategorys = req.getExcludedAdCategoryList();
            if (excludedAdCategorys.isEmpty()) {
            	sBuilder.append(spacers);
			} else {
				for (Integer integer : excludedAdCategorys) {
					sBuilder.append(integer).append(charSpacers);
				}
				sBuilder.append(getSubString(sBuilder)).append(spacers);
			}
            
            sBuilder.append(req.getContentCategoriesCount()).append(spacers);
            
            List<ContentCategory> contentCategories = req.getContentCategoriesList();
            StringBuilder contentCategoryIdBuilder = new StringBuilder();
            StringBuilder contentCategoryConfidenceLevelBuilder = new StringBuilder();
            if (contentCategories.isEmpty()) {
				sBuilder.append(spacers);
				sBuilder.append(spacers);
			} else {
				for (ContentCategory contentCategory : contentCategories) {
	            	
	            	if (contentCategory.hasId()) {
						contentCategoryIdBuilder.append(contentCategory.getId()).append(charSpacers);
					} else {
						contentCategoryIdBuilder.append(charSpacers);
					}
	            	if (contentCategory.hasConfidenceLevel()) {
						contentCategoryConfidenceLevelBuilder.append(contentCategory.getConfidenceLevel()).append(charSpacers);
					} else {
						contentCategoryIdBuilder.append(charSpacers);
					}
				}
				sBuilder.append(getSubString(contentCategoryIdBuilder)).append(spacers);
				sBuilder.append(getSubString(contentCategoryConfidenceLevelBuilder)).append(spacers);
			}
            
            //获取移动设备的信息
            Mobile mobile = req.getMobile();
            if (mobile.hasIsApp()) {
				sBuilder.append(mobile.getIsApp()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (mobile.hasAdNum()) {
				sBuilder.append(mobile.getAdNum()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            ProtocolStringList mobileAdKeywords = mobile.getAdKeywordList();
            if (mobileAdKeywords.isEmpty()) {
				sBuilder.append(spacers);
			} else {
				StringBuilder adKeywordsBuilder = new StringBuilder();
				for (String string : mobileAdKeywords) {
					adKeywordsBuilder.append(string).append(charSpacers);
				}
				sBuilder.append(getSubString(adKeywordsBuilder)).append(spacers);
			}
            
            if (mobile.hasPackageName()) {
				sBuilder.append(mobile.getPackageName()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            Device device = mobile.getDevice();
            if (device.hasPlatform()) {
				sBuilder.append(device.getPlatform()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasBrand()) {
				sBuilder.append(device.getBrand()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasModel()) {
				sBuilder.append(device.getModel()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasOs()) {
				sBuilder.append(device.getOs()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasOsVersion()) {
				sBuilder.append(device.getOsVersion()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
           
            if (device.hasNetwork()) {
				sBuilder.append(device.getNetwork()).append(spacers); 
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasOperator()) {
				sBuilder.append(device.getOperator()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasLongitude()) {
				sBuilder.append(device.getLongitude()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasLatitude()) {
				sBuilder.append(device.getLatitude()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasDeviceSize()) {
            	sBuilder.append(device.getDeviceSize()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasDevicePixelRatio()) {
				sBuilder.append(device.getDevicePixelRatio()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            if (device.hasDeviceId()) {
				sBuilder.append(device.getDeviceId()).append(spacers);
			} else {
				sBuilder.append(spacers);
			}
            
            
            if (req.hasVideo()) {
            	Video video = req.getVideo();
            	List<VideoFormat> videoFormatList = video.getVideoFormatList();
            	if (videoFormatList.isEmpty()) {
					sBuilder.append(spacers);
				} else {
					for (VideoFormat videoFormat : videoFormatList) {
						sBuilder.append(videoFormat.name()).append(charSpacers);
					}
					getSubString(sBuilder).append(spacers);
				}
            	
            	if (video.hasContent()) {
					sBuilder.append(video.getContent());
					Content content = video.getContent();
					if (content.hasTitle()) {
						sBuilder.append(content.getTitle()).append(spacers);
					} else {
						sBuilder.append(spacers);
					}
					
					if (content.hasDuration()) {
						sBuilder.append(content.getDuration()).append(spacers);
					} else {
						sBuilder.append(spacers);
					}
					
					ProtocolStringList keywords = content.getKeywordsList();
					if (keywords.isEmpty()) {
						sBuilder.append(spacers);
					} else {
						for (String string : keywords) {
							sBuilder.append(string).append(charSpacers);
						}
						getSubString(sBuilder).append(spacers);
					}
					
					
					
				} else {
					sBuilder.append(spacers).append(spacers).append(spacers).append(spacers);
				}
            	
            	
            	if (video.hasVideoadStartDelay()) {
					sBuilder.append(video.getVideoadStartDelay()).append(spacers);
				} else {
					sBuilder.append(spacers);
				}
            	
            	if (video.hasVideoadSectionStartDelay()) {
					
            		sBuilder.append(video.getVideoadSectionStartDelay()).append(spacers);
				} else {
					sBuilder.append(spacers);
				}
            	
            	if (video.hasMinAdDuration()) {
					
            		sBuilder.append(video.getMinAdDuration()).append(spacers);
				} else {
					sBuilder.append(spacers);
				}
            	
            	if (video.hasMaxAdDuration()) {
					sBuilder.append(video.getMaxAdDuration()).append(spacers);
				} else {
					sBuilder.append(spacers);
				}
            	
            	if (video.hasProtocol()) {
					
            		sBuilder.append(video.getProtocol()).append(spacers);
				} else {
					sBuilder.append(spacers);
				}
			} else {
				sBuilder.append(spacers).append(spacers).append(spacers).append(spacers).append(spacers)
					.append(spacers).append(spacers).append(spacers).append(spacers).append(spacers);
			}
           
            System.out.println(sBuilder.toString());
            System.out.println(sBuilder.toString().getBytes());
            HexDump.dump(sBuilder.toString().getBytes(), 0, System.out, 0);
            OutputStream out = socket.getOutputStream();
            out.write(result);
            socket.close();
            count++;
			}
        	System.out.println(new Date());
        	System.out.println(count);
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
