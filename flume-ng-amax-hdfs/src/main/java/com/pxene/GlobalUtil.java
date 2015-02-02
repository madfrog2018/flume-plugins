package com.pxene;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.pxene.TanxBidding;
import com.pxene.TanxBidding.BidRequest.AdzInfo;
import com.pxene.TanxBidding.BidRequest.ContentCategory;
import com.pxene.TanxBidding.BidRequest.Mobile;
import com.pxene.TanxBidding.BidRequest.UserAttribute;
import com.pxene.TanxBidding.BidRequest.Video;
import com.pxene.TanxBidding.BidRequest.Mobile.Device;
import com.pxene.TanxBidding.BidRequest.Video.Content;
import com.pxene.TanxBidding.BidRequest.Video.VideoFormat;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by young on 2015/1/19.
 */
public class GlobalUtil {

    private static final Logger logger = LoggerFactory.getLogger(GlobalUtil.class);
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

    public static Event buildMessage(AvroFlumeEvent avroEvent) {

        byte[] dataLengthBytes = new byte[4];
        byte[] dateLongBytes = new byte[8];

        ByteBuffer buffer = avroEvent.getBody();
        getBytes(buffer, 0, dataLengthBytes, 0, 4);
        int datalength = GlobalUtil.byteArrayToInt(dataLengthBytes);
        getBytes(buffer, 4, dateLongBytes, 0, 8);
        long dateLong = GlobalUtil.byteArrayToLong(dateLongBytes);

        byte[] dataBytes = new byte[datalength];
        getBytes(buffer, 12, dataBytes, 0, datalength);
        Character spacers = 0x09;
        Character charSpacers= 0x01;
        Character NULL = 0x02;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(dateLong).append(spacers);
        JSONObject jsonObject = JSONObject.fromObject(new String(dataBytes, 0,
                datalength, Charset.defaultCharset()));
        @SuppressWarnings("rawtypes")
        Set keySet = jsonObject.keySet();
        String bcatStr = null;
        String badvStr = null;
        List<Imp> lists = new ArrayList<Imp>();
        App app = null;
        Device device = null;
        for (Object object : keySet) {
            if (jsonObject.get(object) instanceof String) {

                stringBuilder.append(jsonObject.getString((String)object)).append(spacers);
            }

            if (jsonObject.get(object) instanceof JSONArray) {

                JSONArray jArray = jsonObject.getJSONArray((String)object);
                for (Object obj : jArray) {

                    if (object.toString().equals("bcat")) {

                        bcatStr += ((String)obj + charSpacers);
                    } else if (object.toString().equals("badv")) {

                        badvStr += ((String)obj + charSpacers);
                    } else {

                        Imp impClass = (Imp) JSONObject.toBean((JSONObject)obj, Imp.class);
                        lists.add(impClass);
                    }
                }
            }

            if (object.toString().equals("app")) {

                app = (App) JSONObject.toBean(JSONObject.fromObject(jsonObject.get(object)), App.class);
            }

            if (object.toString().equals("device")) {

                device = (Device) JSONObject.toBean(JSONObject.fromObject(jsonObject.get(object)), Device.class);
            }
        }
        //目前该列表中只有一个元素，简化处理。
        stringBuilder.append(lists.get(0).toString()).append(spacers).append(app.toString()).append(spacers).append(device.toString());
        if (bcatStr != null) {

            stringBuilder.append(spacers).append(bcatStr.substring(0, bcatStr.length() -1));
        } else {
        	stringBuilder.append(spacers).append(NULL);
		}
        if (badvStr != null) {

            stringBuilder.append(spacers).append(badvStr.substring(0, badvStr.length() -1));
        } else {
        	stringBuilder.append(spacers).append(NULL);
		}
        logger.info(stringBuilder.toString());
        return EventBuilder.withBody(stringBuilder.toString().getBytes(),
                toStringMap(avroEvent.getHeaders()));


    }

    public static void getBytes(ByteBuffer buffer, int index, byte[] dst, int dstIndex, int length) {
        ByteBuffer data = buffer.duplicate();
        try {
            data.limit(index + length).position(index);
        } catch (IllegalArgumentException e) {
            throw new IndexOutOfBoundsException("Too many bytes to read - Need "
                    + (index + length) + ", maximum is " + data.limit());
        }
        data.get(dst, dstIndex, length);
    }

    /**
     * Helper function to convert a map of CharSequence to a map of String.
     */
    private static Map<String, String> toStringMap(
            Map<CharSequence, CharSequence> charSeqMap) {
        Map<String, String> stringMap =
                new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }
    
    public static Event buildTanxMessage(AvroFlumeEvent avroEvent) throws InvalidProtocolBufferException {
    	
        byte[] dateLongBytes = new byte[8];
        byte[] dataLengthBytes = new byte[4];
        ByteBuffer buffer = avroEvent.getBody();
        getBytes(buffer, 0, dataLengthBytes, 0, 4);
        int datalength = GlobalUtil.byteArrayToInt(dataLengthBytes);
        getBytes(buffer, 4, dateLongBytes, 0, 8);
        long dateLong = GlobalUtil.byteArrayToLong(dateLongBytes);
        logger.info("parse the dataLength is " + datalength);
        byte[] dataBytes = new byte[datalength];
        getBytes(buffer, 12, dataBytes, 0, datalength);
        TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(dataBytes);
        Character spacers = 0x09;
        Character charSpacers = 0x01;
        Character NULL = 0x02;
        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append(dateLong).append(spacers);
        sBuilder.append(req.getVersion()).append(spacers);
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
			getSubString(sBuilder).append(spacers);
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

        
        return EventBuilder.withBody(getSubString(sBuilder).toString().getBytes(), 
        		toStringMap(avroEvent.getHeaders()));
        
		
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
