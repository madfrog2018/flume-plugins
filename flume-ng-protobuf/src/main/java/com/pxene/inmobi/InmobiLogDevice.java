package com.pxene.inmobi;

import java.util.List;

/**
 * @author shanhongshu 
 * 2015-04-10
 */
public class InmobiLogDevice {

	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
    
	private Integer dnt;
	private String ua;
	private String ip;
	private String make;
	private String model;
	private String os;
	private String osv;
	private String dpidsha1;
	private String dpidmd5;
	private Integer devicetype;
	private InmobiLogGeo geo;
	private InmobiLogExt ext;
	public Integer getDnt() {
		return dnt;
	}
	public void setDnt(Integer dnt) {
		this.dnt = dnt;
	}
	public String getUa() {
		return ua;
	}
	public void setUa(String ua) {
		this.ua = ua;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getMake() {
		return make;
	}
	public void setMake(String make) {
		this.make = make;
	}
	public String getModel() {
		return model;
	}
	public void setModel(String model) {
		this.model = model;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getOsv() {
		return osv;
	}
	public void setOsv(String osv) {
		this.osv = osv;
	}
	public String getDpidsha1() {
		return dpidsha1;
	}
	public void setDpidsha1(String dpidsha1) {
		this.dpidsha1 = dpidsha1;
	}
	public String getDpidmd5() {
		return dpidmd5;
	}
	public void setDpidmd5(String dpidmd5) {
		this.dpidmd5 = dpidmd5;
	}
	public Integer getDevicetype() {
		return devicetype;
	}
	public void setDevicetype(Integer devicetype) {
		this.devicetype = devicetype;
	}
	public InmobiLogGeo getGeo() {
		return geo;
	}
	public void setGeo(InmobiLogGeo geo) {
		this.geo = geo;
	}
	public InmobiLogExt getExt() {
		return ext;
	}
	public void setExt(InmobiLogExt ext) {
		this.ext = ext;
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.isNull(this.getDnt())).append(spacers)
		.append(this.isNull(this.getUa())).append(spacers)
		.append(this.isNull(this.getIp())).append(spacers)
		.append(this.isNull(this.getMake())).append(spacers)
		.append(this.isNull(this.getModel())).append(spacers)
		.append(this.isNull(this.getOs())).append(spacers)
		.append(this.isNull(this.getOsv())).append(spacers)
		.append(this.isNull(this.getDpidsha1())).append(spacers)
		.append(this.isNull(this.getDpidmd5())).append(spacers)
		.append(this.isNull(this.getDevicetype())).append(spacers)
		.append(this.isNull(this.getGeo())).append(spacers)
		.append(this.isNull(this.getExt()));
		
		return sb.toString();
	}
	
	@SuppressWarnings("unchecked")
	private Object isNull(Object obj) {

		String regexStr = String.valueOf(spacers); 
		if (null == obj) {
			return NULL;
		}
		if (obj instanceof List) {
			String result = "";
			for (String string : (List<String>)obj) {

				if (string.indexOf(regexStr) > -1) {
					string.replaceAll(regexStr, "");
				}
				result += (string + charSpacers);
			}
			return result.substring(0, result.length() -1);
		}
		if (obj instanceof String) {
			
			 return ((String)obj).replaceAll(regexStr, "");
		}
		return obj;
	}
	
}
