package com.pxene.inmobi;

import java.util.List;

/**
 * @author shanhongshu 
 * 2015-04-10
 */
public class InmobiLogGeo {
	
	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
    
	private Double lat;
	private Double lng;
	private String country;
	private Integer type;
	
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public Double getLng() {
		return lng;
	}
	public void setLng(Double lng) {
		this.lng = lng;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public Integer getType() {
		return type;
	}
	public void setType(Integer type) {
		this.type = type;
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.isNull(this.getLat())).append(spacers)
		.append(this.isNull(this.getLng())).append(spacers)
		.append(this.isNull(this.getCountry())).append(spacers)
		.append(this.isNull(this.getType())).append(spacers);
		
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
