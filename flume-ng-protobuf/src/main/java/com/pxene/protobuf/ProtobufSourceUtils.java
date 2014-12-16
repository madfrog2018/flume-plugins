/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pxene.protobuf;

import org.apache.commons.io.HexDump;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.source.SyslogSourceConfigurationConstants;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.pxene.protobuf.TanxBidding.BidRequest.AdzInfo;
import com.pxene.protobuf.TanxBidding.BidRequest.ContentCategory;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile;
import com.pxene.protobuf.TanxBidding.BidRequest.UserAttribute;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile.Device;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProtobufSourceUtils {
    final public static String SYSLOG_TIMESTAMP_FORMAT_RFC5424_2 = "yyyy-MM-dd'T'HH:mm:ss.SZ";
    final public static String SYSLOG_TIMESTAMP_FORMAT_RFC5424_1 = "yyyy-MM-dd'T'HH:mm:ss.S";
    final public static String SYSLOG_TIMESTAMP_FORMAT_RFC5424_3 = "yyyy-MM-dd'T'HH:mm:ssZ";
    final public static String SYSLOG_TIMESTAMP_FORMAT_RFC5424_4 = "yyyy-MM-dd'T'HH:mm:ss";
    final public static String SYSLOG_TIMESTAMP_FORMAT_RFC3164_1 = "yyyyMMM d HH:mm:ss";

    final public static String SYSLOG_MSG_RFC5424_0 =
            "(?:\\<\\d{1,3}\\>\\d?\\s?)" + // priority
      /* yyyy-MM-dd'T'HH:mm:ss.SZ or yyyy-MM-dd'T'HH:mm:ss.S+hh:mm or - (null stamp) */
                    "(?:" +
                    "(\\d{4}[-]\\d{2}[-]\\d{2}[T]\\d{2}[:]\\d{2}[:]\\d{2}" +
                    "(?:\\.\\d{1,6})?(?:[+-]\\d{2}[:]\\d{2}|Z)?)|-)" + // stamp
                    "\\s" + // separator
                    "(?:([\\w][\\w\\d\\.@-]*)|-)" + // host name or - (null)
                    "\\s" + // separator
                    "(.*)$"; // body

    final public static String SYSLOG_MSG_RFC3164_0 =
            "(?:\\<\\d{1,3}\\>\\d?\\s?)" +
                    // stamp MMM d HH:mm:ss, single digit date has two spaces
                    "([A-Z][a-z][a-z]\\s{1,2}\\d{1,2}\\s\\d{2}[:]\\d{2}[:]\\d{2})" +
                    "\\s" + // separator
                    "([\\w][\\w\\d\\.@-]*)" + // host
                    "\\s(.*)$";  // body

    final public static int SYSLOG_TIMESTAMP_POS = 1;
    final public static int SYSLOG_HOSTNAME_POS = 2;
    final public static int SYSLOG_BODY_POS = 3;

    private Mode m = Mode.START;
    private StringBuilder prio = new StringBuilder();
    private ByteArrayOutputStream baos;
    private static final Logger logger = LoggerFactory
            .getLogger(ProtobufSourceUtils.class);

    final public static String SYSLOG_FACILITY = "Facility";
    final public static String SYSLOG_SEVERITY = "Severity";
    final public static String EVENT_STATUS = "flume.syslog.status";
    final public static Integer MIN_SIZE = 10;
    final public static Integer DEFAULT_SIZE = 4096;
    private final boolean isUdp;
    private boolean isBadEvent;
    private boolean isIncompleteEvent;
    private Integer maxSize;
    private boolean keepFields;

    private class SyslogFormatter {
        public Pattern regexPattern;
        public ArrayList<String> searchPattern = new ArrayList<String>();
        public ArrayList<String> replacePattern = new ArrayList<String>();
        public ArrayList<SimpleDateFormat> dateFormat = new ArrayList<SimpleDateFormat>();
        public boolean addYear;
    }
    private ArrayList<SyslogFormatter> formats = new ArrayList<SyslogFormatter>();

    private String timeStamp = null;
    private String hostName = null;
    private String msgBody = null;

    public ProtobufSourceUtils() {
        this(false);
    }

    public ProtobufSourceUtils(boolean isUdp) {
        this(DEFAULT_SIZE, SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS, isUdp);
    }

    public ProtobufSourceUtils(Integer eventSize, boolean keepFields, boolean isUdp) {
        this.isUdp = isUdp;
        isBadEvent = false;
        isIncompleteEvent = false;
        maxSize = (eventSize < MIN_SIZE) ? MIN_SIZE : eventSize;
        baos = new ByteArrayOutputStream(eventSize);
        this.keepFields = keepFields;
        initHeaderFormats();
    }

    // extend the default header formatter
    public void addFormats(Map<String, String> formatProp) {
        if (formatProp.isEmpty() || !formatProp.containsKey(
                SyslogSourceConfigurationConstants.CONFIG_REGEX)) {
            return;
        }
        SyslogFormatter fmt1 = new SyslogFormatter();
        fmt1.regexPattern = Pattern.compile( formatProp.get(
                SyslogSourceConfigurationConstants.CONFIG_REGEX) );
        if (formatProp.containsKey(
                SyslogSourceConfigurationConstants.CONFIG_SEARCH)) {
            fmt1.searchPattern.add(formatProp.get(
                    SyslogSourceConfigurationConstants.CONFIG_SEARCH));
        }
        if (formatProp.containsKey(
                SyslogSourceConfigurationConstants.CONFIG_REPLACE)) {
            fmt1.replacePattern.add(formatProp.get(
                    SyslogSourceConfigurationConstants.CONFIG_REPLACE));
        }
        if (formatProp.containsKey(
                SyslogSourceConfigurationConstants.CONFIG_DATEFORMAT)) {
            fmt1.dateFormat.add(new SimpleDateFormat(formatProp.get(
                    SyslogSourceConfigurationConstants.CONFIG_DATEFORMAT)));
        }
        formats.add(0, fmt1);
    }

    // setup built-in formats
    private void initHeaderFormats() {
        // setup RFC5424 formater
        SyslogFormatter fmt1 = new SyslogFormatter();
        fmt1.regexPattern = Pattern.compile(SYSLOG_MSG_RFC5424_0);
        // 'Z' in timestamp indicates UTC zone, so replace it it with '+0000' for date formatting
        fmt1.searchPattern.add("Z");
        fmt1.replacePattern.add("+0000");
        // timezone in RFC5424 is [+-]tt:tt, so remove the ':' for java date formatting
        fmt1.searchPattern.add("([+-])(\\d{2})[:](\\d{2})");
        fmt1.replacePattern.add("$1$2$3");
        fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_1));
        fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_2));
        fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_3));
        fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_4));
        fmt1.addYear = false;

        // setup RFC3164 formater
        SyslogFormatter fmt2 = new SyslogFormatter();
        fmt2.regexPattern = Pattern.compile(SYSLOG_MSG_RFC3164_0);
        // the single digit date has two spaces, so trim it
        fmt2.searchPattern.add("  ");
        fmt2.replacePattern.add(" ");
        fmt2.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC3164_1));
        fmt2.addYear = true;

        formats.add(fmt1);
        formats.add(fmt2);
    }

    enum Mode {
        START, PRIO, DATA
    };

    public enum SyslogStatus{
        OTHER("Unknown"),
        INVALID("Invalid"),
        INCOMPLETE("Incomplete");

        private final String syslogStatus;

        private SyslogStatus(String status){
            syslogStatus = status;
        }

        public String getSyslogStatus(){
            return this.syslogStatus;
        }
    }

    // create the event from syslog data
    Event buildEvent() {
        byte[] body;
        int pri = 0;
        int sev = 0;
        int facility = 0;

        if(!isBadEvent){
            pri = Integer.parseInt(prio.toString());
            sev = pri % 8;
            facility = pri / 8;
            formatHeaders();
        }

        Map <String, String> headers = new HashMap<String, String>();
        headers.put(SYSLOG_FACILITY, String.valueOf(facility));
        headers.put(SYSLOG_SEVERITY, String.valueOf(sev));
        if ((timeStamp != null) && timeStamp.length() > 0) {
            headers.put("timestamp", timeStamp);
        }
        if ((hostName != null) && (hostName.length() > 0)) {
            headers.put("host", hostName);
        }
        if(isBadEvent){
            logger.warn("Event created from Invalid Syslog data.");
            headers.put(EVENT_STATUS, SyslogStatus.INVALID.getSyslogStatus());
        } else if(isIncompleteEvent){
            logger.warn("Event size larger than specified event size: {}. You should " +
                    "consider increasing your event size.", maxSize);
            headers.put(EVENT_STATUS, SyslogStatus.INCOMPLETE.getSyslogStatus());
        }

        if (!keepFields) {
            if ((msgBody != null) && (msgBody.length() > 0)) {
                body = msgBody.getBytes();
            } else {
                // Parse failed.
                body = baos.toByteArray();
            }
        } else {
            body = baos.toByteArray();
        }
        reset();
        // format the message
        return EventBuilder.withBody(body, headers);
    }

    // Apply each known pattern to message
    private void formatHeaders() {
        String eventStr = baos.toString();
        for(int p=0; p < formats.size(); p++) {
            SyslogFormatter fmt = formats.get(p);
            Pattern pattern = fmt.regexPattern;
            Matcher matcher = pattern.matcher(eventStr);
            if (! matcher.matches()) {
                continue;
            }
            MatchResult res = matcher.toMatchResult();
            for (int grp=1; grp <= res.groupCount(); grp++) {
                String value = res.group(grp);
                if (grp == SYSLOG_TIMESTAMP_POS) {
                    // apply available format replacements to timestamp
                    if (value != null) {
                        for (int sp=0; sp < fmt.searchPattern.size(); sp++) {
                            value = value.replaceAll(fmt.searchPattern.get(sp), fmt.replacePattern.get(sp));
                        }
                        // Add year to timestamp if needed
                        if (fmt.addYear) {
                            value = String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + value;
                        }
                        // try the available time formats to timestamp
                        for (int dt = 0; dt < fmt.dateFormat.size(); dt++) {
                            try {
                                timeStamp = String.valueOf(fmt.dateFormat.get(dt).parse(value).getTime());
                                break; // done. formatted the time
                            } catch (ParseException e) {
                                // Error formatting the timeStamp, try next format
                                continue;
                            }
                        }
                    }
                } else if (grp == SYSLOG_HOSTNAME_POS) {
                    hostName = value;
                } else if (grp == SYSLOG_BODY_POS) {
                    msgBody = value;
                }
            }
            break; // we successfully parsed the message using this pattern
        }
    }

    private void reset(){
        baos.reset();
        m = Mode.START;
        prio.delete(0, prio.length());
        isBadEvent = false;
        isIncompleteEvent = false;
        hostName = null;
        timeStamp = null;
        msgBody = null;
    }

    // extract relevant syslog data needed for building Flume event
    public Event extractEvent(ChannelBuffer in){

    /* for protocol debugging
    ByteBuffer bb = in.toByteBuffer();
    int remaining = bb.remaining();
    byte[] buf = new byte[remaining];
    bb.get(buf);
    HexDump.dump(buf, 0, System.out, 0);
    */

        byte b = 0;
        Event e = null;
        boolean doneReading = false;

        try {
            while (!doneReading && in.readable()) {
                b = in.readByte();
                switch (m) {
                    case START:
                        if (b == '<') {
                            baos.write(b);
                            m = Mode.PRIO;
                        } else if(b == '\n'){
                            //If the character is \n, it was because the last event was exactly
                            //as long  as the maximum size allowed and
                            //the only remaining character was the delimiter - '\n', or
                            //multiple delimiters were sent in a row.
                            //Just ignore it, and move forward, don't change the mode.
                            //This is a no-op, just ignore it.
                            logger.debug("Delimiter found while in START mode, ignoring..");

                        } else {
                            isBadEvent = true;
                            baos.write(b);
                            //Bad event, just dump everything as if it is data.
                            m = Mode.DATA;
                        }
                        break;
                    case PRIO:
                        baos.write(b);
                        if (b == '>') {
                            m = Mode.DATA;
                        } else {
                            char ch = (char) b;
                            prio.append(ch);
                            if (!Character.isDigit(ch)) {
                                isBadEvent = true;
                                //If we hit a bad priority, just write as if everything is data.
                                m = Mode.DATA;
                            }
                        }
                        break;
                    case DATA:
                        // TCP syslog entries are separated by '\n'
                        if (b == '\n') {
                            e = buildEvent();
                            doneReading = true;
                        } else {
                            baos.write(b);
                        }
                        if(baos.size() == this.maxSize && !doneReading){
                            isIncompleteEvent = true;
                            e = buildEvent();
                            doneReading = true;
                        }
                        break;
                }

            }

            // UDP doesn't send a newline, so just use what we received
            if (e == null && isUdp) {
                doneReading = true;
                e = buildEvent();
            }
        } finally {
            // no-op
        }

        return e;
    }

    public Integer getEventSize() {
        return maxSize;
    }

    public void setEventSize(Integer eventSize) {
        this.maxSize = eventSize;
    }

    public void setKeepFields(Boolean keepFields) {
        this.keepFields= keepFields;
    }

    public Event MessageHandle(byte[] bytes) throws InvalidProtocolBufferException {
    	
        TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(bytes);
        StringBuilder sBuilder = new StringBuilder();
        String NULL = "NULL";
        String spacers = "|";
        Character charSpacers = new Character((char) 0x01);
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
			StringBuilder userVerticalStringBuilder = new StringBuilder();
			for (Integer integer : userVertical) {
				sBuilder.append(integer).append(charSpacers);
			}
			sBuilder.append(getSubString(userVerticalStringBuilder)).append(spacers);
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
			
			sBuilder.append(getSubString(userAtrributeIdBuilder)).append(spacers);
			sBuilder.append(getSubString(userAtrributeTimestampBuilder)).append(spacers);
			
		}
        
        ProtocolStringList excludedUrls = req.getExcludedClickThroughUrlList();
        if (excludedUrls.isEmpty()) {
        	sBuilder.append(NULL).append(spacers);
		} else {
			StringBuilder excludedUrlsBuilder = new StringBuilder();
			for (String string : excludedUrls) {
				excludedUrlsBuilder.append(string).append(charSpacers);
			}
			sBuilder.append(getSubString(excludedUrlsBuilder)).append(spacers);
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
				adzInfoIdBuilder.append(adzInfo.getId()).append(charSpacers);
			}
        	if (adzInfo.hasPid()) {
				adzInfoPidBuilder.append(adzInfo.getPid()).append(charSpacers);
			}
        	if (adzInfo.hasSize()) {
				adzInfoSizeBuilder.append(adzInfo.getSize()).append(charSpacers);
			}
        	if (adzInfo.hasAdBidCount()) {
				adzInfoAdBidCountBuilder.append(adzInfo.getAdBidCount()).append(charSpacers);
			}
        	
        	List<Integer> viewTypes = adzInfo.getViewTypeList();
        	for (Integer integer : viewTypes) {
				
        		adzInfoViewTypeBuilder.append(integer).append(charSpacers);
			}
        	
        	List<Integer> excludedFilters = adzInfo.getExcludedFilterList();
        	for (Integer integer1 : excludedFilters) {
				adzInfoExcludedFilterBuilder.append(integer1).append(charSpacers);
			}
        	
        	if (adzInfo.hasMinCpmPrice()) {
				
        		adzInfoMinCPMPriceBuilder.append(adzInfo.getMinCpmPrice()).append(charSpacers);
			}
        	
        	
        	if (adzInfo.hasViewScreen()) {
				adzInfoViewScreenBuilder.append(adzInfo.getViewScreen().name()).append(charSpacers);
			}
        	
        	if (adzInfo.hasPageSessionAdIdx()) {
				adzInfoPageSessionAdIdxbBuilder.append(adzInfo.getPageSessionAdIdx()).append(charSpacers);
			}
		}
        
        sBuilder.append(getSubString(adzInfoIdBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoPidBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoSizeBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoAdBidCountBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoViewTypeBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoExcludedFilterBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoMinCPMPriceBuilder)).append(spacers);
        sBuilder.append(getSubString(adzInfoViewScreenBuilder)).append(spacers);
        if (req.hasPageSessionId()) {
			sBuilder.append(req.getPageSessionId()).append(spacers);
		} else {
			sBuilder.append(NULL).append(spacers);
		}
        
        sBuilder.append(getSubString(adzInfoPageSessionAdIdxbBuilder)).append(spacers);
        
        List<Integer> excludedSensitiveCategorys = req.getExcludedSensitiveCategoryList();
        StringBuilder excludedSensitiveBuilder = new StringBuilder();
        if (excludedSensitiveCategorys.isEmpty()) {
        	sBuilder.append(NULL).append(spacers);
		} else {
			for (Integer integer : excludedSensitiveCategorys) {
				excludedSensitiveBuilder.append(integer).append(charSpacers);
			}
			sBuilder.append(getSubString(excludedSensitiveBuilder)).append(spacers);
		}
        
        List<Integer> excludedAdCategorys = req.getExcludedAdCategoryList();
        StringBuilder excludedAdCategorysBuilder = new StringBuilder();
        if (excludedAdCategorys.isEmpty()) {
        	sBuilder.append(NULL).append(spacers);
		} else {
			for (Integer integer : excludedAdCategorys) {
				excludedAdCategorysBuilder.append(integer).append(charSpacers);
			}
			sBuilder.append(getSubString(excludedAdCategorysBuilder)).append(spacers);
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
       
        if (logger.isDebugEnabled()) {
        	 logger.debug(sBuilder.toString());
             try {
     			HexDump.dump(sBuilder.toString().getBytes(), 0, System.out, 0);
     			
     		} catch (Exception e) {
     			logger.debug("debug info exception is " + e.toString());
     		}
		}
        byte[] body = sBuilder.toString().getBytes();
        logger.info("call eventBuilder");
        logger.info(sBuilder.toString());
		return EventBuilder.withBody(sBuilder.toString(), Charset.defaultCharset());
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


