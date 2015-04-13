package com.pxene.inmobi;

import java.util.List;


/**
 * @author shanhongshu 
 * 2015-04-10
 */
public class InmobiLogImp {
	
	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
    
	private String id;
	private InmobiLogBanner banner;
	private Double bidfloor;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public InmobiLogBanner getBanner() {
		return banner;
	}
	public void setBanner(InmobiLogBanner banner) {
		this.banner = banner;
	}
	public Double getBidfloor() {
		return bidfloor;
	}
	public void setBidfloor(Double bidfloor) {
		this.bidfloor = bidfloor;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.isNull(this.getId())).append(spacers)
			.append(this.isNull(this.getBanner())).append(spacers)
			.append(this.isNull(this.getBidfloor()));
		
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
