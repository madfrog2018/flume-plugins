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

import java.util.List;


public class App {
	
	private static final String spacers = "|";
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
	private String aid;
	private String name;
	private List<String> cat;
	private String ver;
	private String bundle;
	private String itid;
	private Integer paid;
	private String storeurl;
	private String keywords;
	private String pid;
	private String pub;
	public String getAid() {
		return aid;
	}
	public void setAid(String aid) {
		this.aid = aid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getCat() {
		return cat;
	}
	public void setCat(List<String> cat) {
		this.cat = cat;
	}
	public String getVer() {
		return ver;
	}
	public void setVer(String ver) {
		this.ver = ver;
	}
	public String getBundle() {
		return bundle;
	}
	public void setBundle(String bundle) {
		this.bundle = bundle;
	}
	public String getItid() {
		return itid;
	}
	public void setItid(String itid) {
		this.itid = itid;
	}
	public Integer getPaid() {
		return paid;
	}
	public void setPaid(Integer paid) {
		this.paid = paid;
	}
	public String getStoreurl() {
		return storeurl;
	}
	public void setStoreurl(String storeurl) {
		this.storeurl = storeurl;
	}

	public String getKeywords() {
		return keywords;
	}
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public String getPub() {
		return pub;
	}
	public void setPub(String pub) {
		this.pub = pub;
	}

	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.isNull(this.getAid())).append(spacers).append(this.isNull(this.getName())).append(spacers)
		.append(this.isNull(this.getCat())).append(spacers).append(this.isNull(this.getVer())).append(spacers)
		.append(this.isNull(this.getBundle())).append(spacers).append(this.isNull(this.getItid())).append(spacers)
		.append(this.isNull(this.getPaid())).append(spacers);
		sb.append(this.isNull(this.getStoreurl())).append(spacers)
		.append(this.isNull(this.getKeywords()).toString().replace(",".charAt(0), charSpacers))
		.append(spacers).append(this.isNull(this.getPid())).append(spacers).append(this.isNull(this.getPub()));
		return sb.toString();
	}
	
	@SuppressWarnings("unchecked")
	private Object isNull(Object obj) {

		if (null == obj) {
			return NULL;
		}
		if (obj instanceof List) {
			String result = "";
			for (String string : (List<String>)obj) {

				result += (string + charSpacers);
			}
			return result.substring(0, result.length() -1);
		}
		return obj;
	}
}
