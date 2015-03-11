package com.pxene;

/**
 * Created by young on 2015/3/10.
 */
public class AmaxMessage {

    private String id;
    private Imp[] imp;
    private AmaxApp app;
    private AmaxLogDevice device;
    private String[] bcat;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Imp[] getImp() {
        return imp;
    }

    public void setImp(Imp[] imp) {
        this.imp = imp;
    }

    public AmaxApp getApp() {
        return app;
    }

    public void setApp(AmaxApp app) {
        this.app = app;
    }

    public AmaxLogDevice getDevice() {
        return device;
    }

    public void setDevice(AmaxLogDevice device) {
        this.device = device;
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

    private String[] badv;
}