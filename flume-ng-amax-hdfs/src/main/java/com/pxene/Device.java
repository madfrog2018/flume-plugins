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

public class Device {

	private static final Character spacers = 0x09;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
	private String did;
	private String dpid;
	private String mac;
	private String ua;
	private String ip;
	private String country;
	private String carrier;
	private String language;
	private String make;
	private String model;
	private String os;
	private String osv;
	private Integer connectiontype;
	private Integer devicetype;
	private String loc;
	private Float density;
	private Integer sw;
	private Integer sh;
	private Integer orientation;
	public String getDid() {
		return did;
	}
	public void setDid(String did) {
		this.did = did;
	}
	public String getDpid() {
		return dpid;
	}
	public void setDpid(String dpid) {
		this.dpid = dpid;
	}
	public String getMac() {
		return mac;
	}
	public void setMac(String mac) {
		this.mac = mac;
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
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getCarrier() {
		return carrier;
	}
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
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
	public Integer getConnectiontype() {
		return connectiontype;
	}
	public void setConnectiontype(Integer connectiontype) {
		this.connectiontype = connectiontype;
	}
	public Integer getDevicetype() {
		return devicetype;
	}
	public void setDevicetype(Integer devicetype) {
		this.devicetype = devicetype;
	}
	public String getLoc() {
		return loc;
	}
	public void setLoc(String loc) {
		this.loc = loc;
	}
	public Float getDensity() {
		return density;
	}
	public void setDensity(Float density) {
		this.density = density;
	}
	public Integer getSw() {
		return sw;
	}
	public void setSw(Integer sw) {
		this.sw = sw;
	}
	public Integer getSh() {
		return sh;
	}
	public void setSh(Integer sh) {
		this.sh = sh;
	}
	public Integer getOrientation() {
		return orientation;
	}
	public void setOrientation(Integer orientation) {
		this.orientation = orientation;
	}
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.isNull(this.getDid())).append(spacers).append(this.isNull(this.getDpid())).append(spacers)
		.append(this.isNull(this.getMac())).append(spacers).append(this.isNull(this.getUa())).append(spacers)
		.append(this.isNull(this.getIp())).append(spacers).append(this.isNull(this.getCountry())).append(spacers)
		.append(this.isNull(this.getCarrier())).append(spacers).append(this.isNull(this.getLanguage())).append(spacers)
		.append(this.isNull(this.getMake())).append(spacers).append(this.isNull(this.getModel())).append(spacers)
		.append(this.isNull(this.getOs())).append(spacers).append(this.isNull(this.getOsv())).append(spacers)
		.append(this.isNull(this.getConnectiontype())).append(spacers).append(this.isNull(this.getDevicetype())).append(spacers)
		.append(this.isNull(this.getSw())).append(spacers).append(this.isNull(this.getSh())).append(spacers)
		.append(this.isNull(this.getOrientation())).append(spacers).append(this.isNull(this.getDensity()))
		.append(spacers).append(this.isNull(this.getLoc()));
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
