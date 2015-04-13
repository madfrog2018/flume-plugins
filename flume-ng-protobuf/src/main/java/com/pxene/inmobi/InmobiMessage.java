package com.pxene.inmobi;

import java.util.List;

/**
 * @author shanhongshu 
 * 2015-04-10
 */
public class InmobiMessage {

	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
    
	private String id;
	private Integer at;
	private Integer tmax;
	private String[] cur;
	private String[] bcat;
	private String[] badv;
	
	private InmobiLogImp[] imp;
	
	private InmobiLogApp app;
	
	private InmobiLogDevice[] device;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getAt() {
		return at;
	}

	public void setAt(Integer at) {
		this.at = at;
	}

	public Integer getTmax() {
		return tmax;
	}

	public void setTmax(Integer tmax) {
		this.tmax = tmax;
	}

	public String[] getCur() {
		return cur;
	}

	public void setCur(String[] cur) {
		this.cur = cur;
	}

	public String[] getBcat() {
		return bcat;
	}

	public void setBcat(String[] bcat) {
		this.bcat = bcat;
	}

	public String[] getBadv() {
		return badv;
	}

	public void setBadv(String[] badv) {
		this.badv = badv;
	}

	public InmobiLogImp[] getImp() {
		return imp;
	}

	public void setImp(InmobiLogImp[] imp) {
		this.imp = imp;
	}

	public InmobiLogApp getApp() {
		return app;
	}

	public void setApp(InmobiLogApp app) {
		this.app = app;
	}

	public InmobiLogDevice[] getDevice() {
		return device;
	}

	public void setDevice(InmobiLogDevice[] device) {
		this.device = device;
	}
		
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.isNull(this.getId())).append(spacers)
			.append(this.isNull(this.getAt())).append(spacers)
			.append(this.isNull(this.getTmax())).append(spacers)
			.append(this.isNull(this.getCur())).append(spacers)
			.append(this.isNull(this.getBcat())).append(spacers)
			.append(this.isNull(this.getBadv())).append(spacers)
			.append(this.isNull(this.getImp())).append(spacers)
			.append(this.isNull(this.getApp())).append(spacers)
			.append(this.isNull(this.getDevice()));
		
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
