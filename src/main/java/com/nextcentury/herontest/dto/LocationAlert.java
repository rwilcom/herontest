package com.nextcentury.herontest.dto;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public class LocationAlert extends ByteSerializable {

    public LocationAlert(){}

    public Date alertDateTime;
    public String eventUuid;
    public String alertDescription;

    @Override
    public String toString(){
        return "alertDateTime:"+alertDateTime+
               ";eventUuid:"+eventUuid +
               ";alertDescription:"+alertDescription;
    }
    
    /**
     * 
     * @return 
     */    
    public String toStringTuple(){
               
        return "["+"LOCATION_ALERT"+","
               +alertDateTime+","
               +eventUuid+","
               +alertDescription+"]";
    }
    
    
  
    static public LocationAlert getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (LocationAlert)ByteSerializable.getFromBytes(objBytes);
    }
}
