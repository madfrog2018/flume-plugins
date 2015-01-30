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

public class Imp {
	
	private static final Character spacers = 0x03;
    private static final Character charSpacers= 0x01;
    private static final Character NULL = 0x02;
	private Integer splash;
	private String impid;
	private Integer bidfloor;
	private String bidfloorcur;
	private Integer w;
	private Integer h;
	private Integer pos;
	private List<String> btype;
	private List<String> battr;
	private Integer instl;
	public Integer getSplash() {
		return splash;
	}
	public void setSplash(Integer splash) {
		this.splash = splash;
	}
	public String getImpid() {
		return impid;
	}
	public void setImpid(String impid) {
		this.impid = impid;
	}
	public Integer getBidfloor() {
		return bidfloor;
	}
	public void setBidfloor(Integer bidfloor) {
		this.bidfloor = bidfloor;
	}
	public String getBidfloorcur() {
		return bidfloorcur;
	}
	public void setBidfloorcur(String bidfloorcur) {
		this.bidfloorcur = bidfloorcur;
	}
	public Integer getW() {
		return w;
	}
	public void setW(Integer w) {
		this.w = w;
	}
	public Integer getH() {
		return h;
	}
	public void setH(Integer h) {
		this.h = h;
	}
	public Integer getPos() {
		return pos;
	}
	public void setPos(Integer pos) {
		this.pos = pos;
	}
	public List<String> getBtype() {
		return btype;
	}
	public void setBtype(List<String> btype) {
		this.btype = btype;
	}
	public List<String> getBattr() {
		return battr;
	}
	public void setBattr(List<String> battr) {
		this.battr = battr;
	}
	public Integer getInstl() {
		return instl;
	}
	public void setInstl(Integer instl) {
		this.instl = instl;
	}
	
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.isNull(this.getSplash())).append(spacers).append(this.isNull(this.getImpid())).append(spacers)
		.append(this.isNull(this.getBidfloor())).append(spacers).append(this.isNull(this.getBidfloorcur())).append(spacers)
		.append(this.isNull(this.getW())).append(spacers).append(this.isNull(this.getH())).append(spacers)
		.append(this.isNull(this.getInstl())).append(spacers).append(this.isNull(this.getPos())).append(spacers)
		.append(this.isNull(this.getBtype())).append(spacers).append(this.isNull(this.getBattr()));
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
