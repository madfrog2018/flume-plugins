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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.pxene.protobuf.TanxBidding.BidRequest.*;
import com.pxene.protobuf.TanxBidding.BidRequest.Mobile.Device;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.Content;
import com.pxene.protobuf.TanxBidding.BidRequest.Video.VideoFormat;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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

        boolean isDoning = false;
        Event e = null;
            if (bytes.length <= 12) {
                //前12字节是请求时间和数据长度的标识
                return EventBuilder.withBody("", Charset.defaultCharset());
            }
            int timeLength = 8;
            byte[] reqTimeBytes = getDataFromByteArray(bytes, 0, timeLength);
            long dateLong = byteArrayToLong(reqTimeBytes);
//        long reqTime = reqTimeByte
            int dataContainerLength = 4;

            byte[] dataLengthBytes = getDataFromByteArray(bytes, timeLength, dataContainerLength);

            int dataLength = byteArrayToInt(dataLengthBytes);
            byte[] reqBytes = getDataFromByteArray(bytes, (timeLength + dataContainerLength), dataLength);
            logger.info("data length is " + reqBytes.length);
            TanxBidding.BidRequest req = TanxBidding.BidRequest.parseFrom(reqBytes);
            String spacers = "|";
            Character charSpacers = 0x01;
            StringBuilder sBuilder = new StringBuilder();
            sBuilder.append(dateLong).append(spacers);
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
                for (int j = 0; j < mergeredValues.length - 1; j++) {

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


            logger.debug(sBuilder.toString());
            e = EventBuilder.withBody(getSubString(sBuilder).toString(), Charset.defaultCharset());
        return e;
    }
    	
    
    public StringBuilder getSubString(StringBuilder sb){
    	
    	if (sb == null) {
			return null;
		}
    	
    	String subString = sb.toString().substring(0, sb.length()-1);
    	sb.delete(0, sb.length()).append(subString);
    	return sb;
    }

    public byte[] getDataFromByteArray(byte[] bt, int start, int length) {

        baos.reset();
        int end = start + length;
        for (int i = start; i < end; i++) {

            baos.write(bt[i]);
        }

        return baos.toByteArray();
    }

    /**
     * 将8字节的byte数组转成一个long值
     * @param byteArray
     * @return 转换后的long型数值
     */
    public long byteArrayToLong(byte[] byteArray) {
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
    public int byteArrayToInt(byte[] b){
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


