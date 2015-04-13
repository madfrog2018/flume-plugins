package com.pxene.inmobi;

import java.util.List;

public class InmobiLogExt {
	
	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
    
	private String idfasha1;
	private String idfamd5;
	
	public String getIdfasha1() {
		return idfasha1;
	}
	public void setIdfasha1(String idfasha1) {
		this.idfasha1 = idfasha1;
	}
	public String getIdfamd5() {
		return idfamd5;
	}
	public void setIdfamd5(String idfamd5) {
		this.idfamd5 = idfamd5;
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.isNull(this.getIdfasha1())).append(spacers)
			.append(this.isNull(this.getIdfamd5()));
		
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
