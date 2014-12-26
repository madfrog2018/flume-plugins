package com.pxene.protobuf;

import com.google.protobuf.ProtocolStringList;
import com.pxene.protobuf.TanxBidding.BidRequest.*;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile.Device;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.Content;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.VideoFormat;

import org.apache.commons.io.HexDump;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.List;

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
			Socket socket = new Socket("192.168.2.7", 5140);
        	for (int i = 0; i < 3; i++) {
            File file = new File("D:\\git\\flume-plugins\\flume-ng-protobuf\\test.txt");
            @SuppressWarnings("resource")
			FileInputStream inputStream = new FileInputStream(file);
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes);
            HexDump.dump(bytes, 0, System.out, 0);

			int timeLength = 8;
			byte[] reqTimeBytes = getDataFromByteArray(bytes, 0, timeLength);
			long dateLong = byteArrayToLong(reqTimeBytes);
			logger.info("dataLong is " + dateLong);
			int dataContainerLength = 4;

			byte[] dataLengthBytes = getDataFromByteArray(bytes, timeLength, dataContainerLength);
			int dataLength = byteArrayToInt(dataLengthBytes);
			byte[] reqBytes = getDataFromByteArray(bytes, (timeLength + dataContainerLength), dataLength);
			logger.info("data length is " + reqBytes.length);
            String spacers = "|";
            Character charSpacers = 0x01;
            Character NULL = 0x02;
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(reqBytes);
            StringBuilder sBuilder = new StringBuilder();
			sBuilder.append(dateLong).append(spacers);
            sBuilder.append(req.getVersion()).append(spacers);
//            sBuilder.append(count).append(spacers);
            sBuilder.append(req.getBid()).append(spacers);
            
            if (req.hasIsTest()) {
            	sBuilder.append(req.getIsTest()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasIsPing()) {
            	sBuilder.append(req.getIsPing()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasTid()) {
            	sBuilder.append(req.getTid()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasIp()) {
				sBuilder.append(req.getIp()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasUserAgent()) {
				
            	sBuilder.append(req.getUserAgent()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasTimezoneOffset()) {
            	
            	sBuilder.append(req.getTimezoneOffset()).append(spacers);
            } else {
            	sBuilder.append(NULL).append(spacers);
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
            	sBuilder.append(req.getTidVersion()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            
            sBuilder.append(req.getPrivateInfoCount()).append(spacers);
            
            if (req.hasHostedMatchData()) {
				sBuilder.append(req.getHostedMatchData()).append(spacers);
			
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            sBuilder.append(req.getUserAttributeCount()).append(spacers);
            
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
            	sBuilder.append(NULL).append(spacers);
			} else {
				
				for (String string : excludedUrls) {
					sBuilder.append(string).append(charSpacers);
				}
				getSubString(sBuilder).append(spacers);
			}
            
            if (req.hasUrl()) {
				
            	sBuilder.append(req.getUrl()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasCategory()) {
				sBuilder.append(req.getCategory()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasAdxType()) {
				sBuilder.append(req.getAdxType()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasAnonymousId()) {
            	sBuilder.append(req.getAnonymousId()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasDetectedLanguage()) {
            	sBuilder.append(req.getDetectedLanguage()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (req.hasCategoryVersion()) {
				sBuilder.append(req.getCategoryVersion()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
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
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	if (adzInfo.hasPid()) {
	            		adzInfoBuilder.append(adzInfo.getPid()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	if (adzInfo.hasSize()) {
	            		adzInfoBuilder.append(adzInfo.getSize()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	if (adzInfo.hasAdBidCount()) {
	            		adzInfoBuilder.append(adzInfo.getAdBidCount()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	
	            	List<Integer> viewTypes = adzInfo.getViewTypeList();
	            	if (viewTypes.isEmpty()) {
						adzInfoBuilder.append(NULL).append(spacers);
					} else {
						for (Integer integer : viewTypes) {
							
		            		adzInfoViewTypeBuilder.append(integer).append(charSpacers);
						}
						adzInfoBuilder.append(getSubString(adzInfoViewTypeBuilder)).append(spacers);
					}
	            	
	            	
	            	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
	            	if (excludedFilters.isEmpty()) {
	            		adzInfoBuilder.append(NULL).append(spacers);
					} else {
						for (Integer integer : excludedFilters) {
							adzInfoExcludedFilterBuilder.append(integer).append(charSpacers);
						}
						adzInfoBuilder.append(getSubString(adzInfoExcludedFilterBuilder)).append(spacers);
					}
	            	
	            	
	            	if (adzInfo.hasMinCpmPrice()) {
						
	            		adzInfoBuilder.append(adzInfo.getMinCpmPrice()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	
	            	
	            	if (adzInfo.hasViewScreen()) {
	            		adzInfoBuilder.append(adzInfo.getViewScreen().name()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
	            	
	            	if (adzInfo.hasPageSessionAdIdx()) {
	            		adzInfoBuilder.append(adzInfo.getPageSessionAdIdx()).append(spacers);
					} else {
						adzInfoBuilder.append(NULL).append(spacers);
					}
				} else {
					
					String[] values = adzInfoBuilder.toString().split("\\|");
					if (adzInfo.hasId()) {
						mergeredValues[0] = values[0] + charSpacers + adzInfo.getId();
					} else {
						mergeredValues[0] = values[0] + NULL + charSpacers;
					}
	            	if (adzInfo.hasPid()) {
	            		mergeredValues[1] = values[1] + charSpacers + adzInfo.getPid();
					} else {
						mergeredValues[1] = values[1] + NULL + charSpacers;
					}
	            	if (adzInfo.hasSize()) {
	            		mergeredValues[2] = values[2] + charSpacers + adzInfo.getSize();
					} else {
						mergeredValues[2] = values[2] + NULL + charSpacers;
					}
	            	if (adzInfo.hasAdBidCount()) {
	            		mergeredValues[3] = values[3] + charSpacers + adzInfo.getAdBidCount();
					} else {
						mergeredValues[3] = values[3] + NULL + charSpacers;
					}
	            	
	            	List<Integer> viewTypes = adzInfo.getViewTypeList();
	            	if (viewTypes.isEmpty()) {
	            		mergeredValues[4] = values[4] + NULL + charSpacers;
					} else {
						for (Integer integer : viewTypes) {
							
		            		adzInfoViewTypeBuilder.append(integer).append(charSpacers);
						}
						mergeredValues[4] = values[4] + charSpacers + getSubString(adzInfoViewTypeBuilder).toString();
					}
	            	
	            	
	            	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
	            	if (excludedFilters.isEmpty()) {
	            		mergeredValues[5] = values[5] + NULL + charSpacers;
					} else {
						for (Integer integer1 : excludedFilters) {
							adzInfoExcludedFilterBuilder.append(integer1).append(charSpacers);
						}
						mergeredValues[5] = values[5] + charSpacers + getSubString(adzInfoExcludedFilterBuilder).toString();
					}
	            	
	            	
	            	if (adzInfo.hasMinCpmPrice()) {
						
	            		mergeredValues[6] = values[6] + charSpacers + adzInfo.getMinCpmPrice();
					} else {
						mergeredValues[6] = values[6] + NULL + charSpacers;
					}
	            	
	            	
	            	if (adzInfo.hasViewScreen()) {
	            		mergeredValues[7] = values[7] + charSpacers + adzInfo.getViewScreen().name();
					} else {
						mergeredValues[7] = values[7] + NULL + charSpacers;
					}
	            	
	            	if (adzInfo.hasPageSessionAdIdx()) {
	            		mergeredValues[8] = values[8] + charSpacers + adzInfo.getPageSessionAdIdx();
					} else {
						mergeredValues[8] = values[8] + NULL + charSpacers;
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
				sBuilder.append(NULL).append(spacers);
			}
            
            
            List<Integer> excludedSensitiveCategorys = req.getExcludedSensitiveCategoryList();
            
            if (excludedSensitiveCategorys.isEmpty()) {
            	sBuilder.append(NULL).append(spacers);
			} else {
				for (Integer integer : excludedSensitiveCategorys) {
					sBuilder.append(integer).append(charSpacers);
				}
				sBuilder.append(getSubString(sBuilder)).append(spacers);
			}
            
            List<Integer> excludedAdCategorys = req.getExcludedAdCategoryList();
            if (excludedAdCategorys.isEmpty()) {
            	sBuilder.append(NULL).append(spacers);
			} else {
				for (Integer integer : excludedAdCategorys) {
					sBuilder.append(integer).append(charSpacers);
				}
				getSubString(sBuilder).append(spacers);
			}
            
            sBuilder.append(req.getContentCategoriesCount()).append(spacers);
            
            List<ContentCategory> contentCategories = req.getContentCategoriesList();
            StringBuilder contentCategoryIdBuilder = new StringBuilder();
            StringBuilder contentCategoryConfidenceLevelBuilder = new StringBuilder();
            if (contentCategories.isEmpty()) {
				sBuilder.append(NULL).append(spacers);
				sBuilder.append(NULL).append(spacers);
			} else {
				for (ContentCategory contentCategory : contentCategories) {
	            	
	            	if (contentCategory.hasId()) {
						contentCategoryIdBuilder.append(contentCategory.getId()).append(charSpacers);
					} else {
						contentCategoryIdBuilder.append(NULL).append(charSpacers);
					}
	            	if (contentCategory.hasConfidenceLevel()) {
						contentCategoryConfidenceLevelBuilder.append(contentCategory.getConfidenceLevel()).append(charSpacers);
					} else {
						contentCategoryIdBuilder.append(NULL).append(charSpacers);
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
				sBuilder.append(NULL).append(spacers);
			}
            
            if (mobile.hasAdNum()) {
				sBuilder.append(mobile.getAdNum()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (mobile.hasIsFullscreen()) {
				sBuilder.append(mobile.getIsFullscreen()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            
            ProtocolStringList mobileAdKeywords = mobile.getAdKeywordList();
            if (mobileAdKeywords.isEmpty()) {
				sBuilder.append(NULL).append(spacers);
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
				sBuilder.append(NULL).append(spacers);
			}
            
            Device device = mobile.getDevice();
            if (device.hasPlatform()) {
				sBuilder.append(device.getPlatform()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasBrand()) {
				sBuilder.append(device.getBrand()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasModel()) {
				sBuilder.append(device.getModel()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasOs()) {
				sBuilder.append(device.getOs()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasOsVersion()) {
				sBuilder.append(device.getOsVersion()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
           
            if (device.hasNetwork()) {
				sBuilder.append(device.getNetwork()).append(spacers); 
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasOperator()) {
				sBuilder.append(device.getOperator()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasLongitude()) {
            	System.out.println(device.getLongitude());
            	System.out.println(device.getLongitude().equals(""));
				sBuilder.append(device.getLongitude()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasLatitude()) {
				sBuilder.append(device.getLatitude()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasDeviceSize()) {
            	sBuilder.append(device.getDeviceSize()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasDevicePixelRatio()) {
				sBuilder.append(device.getDevicePixelRatio()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            if (device.hasDeviceId()) {
				sBuilder.append(device.getDeviceId()).append(spacers);
			} else {
				sBuilder.append(NULL).append(spacers);
			}
            
            
            if (req.hasVideo()) {
            	Video video = req.getVideo();
            	List<VideoFormat> videoFormatList = video.getVideoFormatList();
            	if (videoFormatList.isEmpty()) {
					sBuilder.append(NULL).append(spacers);
				} else {
					for (VideoFormat videoFormat : videoFormatList) {
						sBuilder.append(videoFormat.name()).append(charSpacers);
					}
					getSubString(sBuilder).append(spacers);
				}
            	
            	if (video.hasContent()) {
					sBuilder.append(video.getContent()).append(spacers);
					Content content = video.getContent();
					if (content.hasTitle()) {
						sBuilder.append(content.getTitle()).append(spacers);
					} else {
						sBuilder.append(NULL).append(spacers);
					}
					
					if (content.hasDuration()) {
						sBuilder.append(content.getDuration()).append(spacers);
					} else {
						sBuilder.append(NULL).append(spacers);
					}
					
					ProtocolStringList keywords = content.getKeywordsList();
					if (keywords.isEmpty()) {
						sBuilder.append(NULL).append(spacers);
					} else {
						for (String string : keywords) {
							sBuilder.append(string).append(charSpacers);
						}
						getSubString(sBuilder).append(spacers);
					}
					
					
					
				} else {
					sBuilder.append(NULL).append(spacers).append(NULL).append(spacers).append(NULL)
					.append(spacers).append(NULL).append(spacers);
				}
            	
            	
            	if (video.hasVideoadStartDelay()) {
					sBuilder.append(video.getVideoadStartDelay()).append(spacers);
				} else {
					sBuilder.append(NULL).append(spacers);
				}
            	
            	if (video.hasVideoadSectionStartDelay()) {
					
            		sBuilder.append(video.getVideoadSectionStartDelay()).append(spacers);
				} else {
					sBuilder.append(NULL).append(spacers);
				}
            	
            	if (video.hasMinAdDuration()) {
					
            		sBuilder.append(video.getMinAdDuration()).append(spacers);
				} else {
					sBuilder.append(NULL).append(spacers);
				}
            	
            	if (video.hasMaxAdDuration()) {
					sBuilder.append(video.getMaxAdDuration()).append(spacers);
				} else {
					sBuilder.append(NULL).append(spacers);
				}
            	
            	if (video.hasProtocol()) {
					
            		sBuilder.append(video.getProtocol()).append(spacers);
				} else {
					sBuilder.append(NULL).append(spacers);
				}
			} else {
				sBuilder.append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers)
					.append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers).append(NULL).append(spacers);
			}
           
            System.out.println(sBuilder.toString());
//            System.out.println(sBuilder.toString().getBytes());
//            HexDump.dump(sBuilder.toString().getBytes(), 0, System.out, 0);
            OutputStream out = socket.getOutputStream();
//            byte[] sbuilderBytes = sBuilder.toString().getBytes();
            
            out.write(bytes);

            count++;
			}
			socket.close();
        	System.out.println(new Date());
        	System.out.println(count);
//			socket.close();
        } catch (IOException e) {
            logger.error("error is " + e.toString());
//        } catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
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
	public static byte[] getDataFromByteArray(byte[] bt, int start, int length) {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(length);
		int end = start + length;
		for (int i = start; i < end; i++) {

			byteArrayOutputStream.write(bt[i]);
		}

		return byteArrayOutputStream.toByteArray();
	}
	/**
	 * 将8字节的byte数组转成一个long值
	 * @param byteArray
	 * @return 转换后的long型数值
	 */
	public static long byteArrayToLong(byte[] byteArray) {
		byte[] a = new byte[8];
		int i = a.length - 1, j = byteArray.length - 1;
		for (; i >= 0; i--, j--) {// 从b的尾部(即int值的低位)开始copy数据
			if (j >= 0)
				a[i] = byteArray[j];
			else
				a[i] = 0;// 如果b.length不足4,则将高位补0
		}
		
//		int j = byteArray.length - 1;
//		for (int i = 0; i < a.length; i++, j--) {
//				
//			if (j > 0) {
//				a[i] = byteArray[j];
//			} else {
//				a[i] = 0;
//			}
//			
//		}
		
		// 注意此处和byte数组转换成int的区别在于，下面的转换中要将先将数组中的元素转换成long型再做移位操作，
		// 若直接做位移操作将得不到正确结果，因为Java默认操作数字时，若不加声明会将数字作为int型来对待，此处必须注意。
		long v0 = (long) (a[0] & 0xff) << 56;// &0xff将byte值无差异转成int,避免Java自动类型提升后,会保留高位的符号位
		long v1 = (long) (a[1] & 0xff) << 48;
		long v2 = (long) (a[2] & 0xff) << 40;
		long v3 = (long) (a[3] & 0xff) << 32;
		long v4 = (long) (a[4] & 0xff) << 24;
		long v5 = (long) (a[5] & 0xff) << 16;
		long v6 = (long) (a[6] & 0xff) << 8;
		long v7 = (long) (a[7] & 0xff);
		return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7;
	}

	/**
	 * 将4字节的byte数组转成一个int值
	 * @param b
	 * @return
	 */
	public static int byteArrayToInt(byte[] b){
		byte[] a = new byte[4];
		int i = a.length - 1,j = b.length - 1;
		for (; i >= 0 ; i--,j--) {//从b的尾部(即int值的低位)开始copy数据
			if(j >= 0)
				a[i] = b[j];
			else
				a[i] = 0;//如果b.length不足4,则将高位补0
		}
		int v0 = (a[0] & 0xff) << 24;//&0xff将byte值无差异转成int,避免Java自动类型提升后,会保留高位的符号位
		int v1 = (a[1] & 0xff) << 16;
		int v2 = (a[2] & 0xff) << 8;
		int v3 = (a[3] & 0xff) ;
		return v0 + v1 + v2 + v3;
	}
}
