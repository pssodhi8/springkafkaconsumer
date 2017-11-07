package com.mplatform.manalytics.mstore.datamatcher.entities;

import java.io.Serializable;

public class KafkaResponse implements Serializable{
    private static final long serialVersionUID = -2883652015441502669L;
    String vendor;
    String dataCategory;
    String fileDateStartTimestamp;
    String filetDateEndTimestamp;
    String fileDateEndTimestamp;
    
    public String getVendor() {
      return vendor;
    }
    public void setVendor(String vendor) {
      this.vendor = vendor;
    }
    public String getDataCategory() {
      return dataCategory;
    }
    public void setDataCategory(String dataCategory) {
      this.dataCategory = dataCategory;
    }
    public String getFileDateStartTimestamp() {
      return fileDateStartTimestamp;
    }
    public void setFileDateStartTimestamp(String fileDateStartTimestamp) {
      this.fileDateStartTimestamp = fileDateStartTimestamp;
    }
    public String getFiletDateEndTimestamp() {
      return filetDateEndTimestamp;
    }
    public void setFiletDateEndTimestamp(String filetDateEndTimestamp) {
      this.filetDateEndTimestamp = filetDateEndTimestamp;
    }
    public String getFileDateEndTimestamp() {
      return fileDateEndTimestamp;
    }
    public void setFileDateEndTimestamp(String fileDateEndTimestamp) {
      this.fileDateEndTimestamp = fileDateEndTimestamp;
    }
}
