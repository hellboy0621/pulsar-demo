package com.xtransformers.domain;

import java.io.Serializable;

public class Equipment implements Serializable {

    private String id;
    private String name;
    private String sn;
    private String type;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSn() {
        return sn;
    }

    public void setSn(String sn) {
        this.sn = sn;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Equipment{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sn='" + sn + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
