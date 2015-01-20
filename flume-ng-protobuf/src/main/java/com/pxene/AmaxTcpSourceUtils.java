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
package com.pxene;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pxene.TanxBidding.BidRequest.Mobile.Device;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AmaxTcpSourceUtils {
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
            .getLogger(AmaxTcpSourceUtils.class);

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

    public AmaxTcpSourceUtils() {
        this(false);
    }

    public AmaxTcpSourceUtils(boolean isUdp) {
        this(DEFAULT_SIZE, SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS, isUdp);
    }

    public AmaxTcpSourceUtils(Integer eventSize, boolean keepFields, boolean isUdp) {
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
    public Event extractEvent(ByteArrayOutputStream boss ,ChannelBuffer in) throws InvalidProtocolBufferException {

    /* for protocol debugging
    ByteBuffer bb = in.toByteBuffer();
    int remaining = bb.remaining();
    byte[] buf = new byte[remaining];
    bb.get(buf);
    HexDump.dump(buf, 0, System.out, 0);
    */
//        byte b = 0;
        Event e = null;
        byte[] dateTimeBytes = new byte[8];
        byte[] dataBytes = new byte[4];
        int dataLength = 0;
        int i = 0;
        long dateLong = 0l;
        try {
        	logger.debug("boss size is " + boss.size());
            if (0 != boss.size()){
                in.setBytes(0,boss.toByteArray());
                boss = new ByteArrayOutputStream();
            }
            
            while (in.readable()) {
                if (0 == dataLength && 0l == dateLong) {
            		in.readBytes(dateTimeBytes, 0, 8);
                    dateLong = byteArrayToLong(dateTimeBytes);
                    logger.info("dateLong is " + dateLong);
                    in.readBytes(dataBytes, 0, 4);
                    dataLength = byteArrayToInt(dataBytes);
                    logger.info("data length is " + dataLength);
                } else {
                    byte b = in.readByte();
                    baos.write(b);
                    i++;
                    if (dataLength == i) {
                        e = buildMessage(dateLong, baos.toByteArray());
                        dataLength = 0;
                        dateLong = 0l;
                        i = 0;
                        baos.reset();
                    }
                }
            }
            if(0 != baos.size()){
            	boss.write(baos.toByteArray());
            }
        }catch(InvalidProtocolBufferException e1){
            logger.error(e1.getMessage());
        } catch (IOException e1) {
        	logger.error(e1.getMessage());
		} finally {

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

	public Event buildMessage(long dateLong, byte[] reqBytes) throws InvalidProtocolBufferException {

		String spacers = "|";
        Character charSpacers = 0x01;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(dateLong).append(spacers);

        JSONObject jsonObject = JSONObject.fromObject(reqBytes);
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
		}
        if (badvStr != null) {
			
        	stringBuilder.append(spacers).append(badvStr.substring(0, badvStr.length() -1));
		}
        return EventBuilder.withBody(stringBuilder.toString(), Charset.defaultCharset());

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


