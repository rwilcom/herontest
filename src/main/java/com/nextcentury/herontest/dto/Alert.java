package com.nextcentury.herontest.dto;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public abstract class Alert extends ByteSerializable {

    public Alert(){}

    public Date alertDateTime;
    public String eventUuid;
    public String alertDescription;

    public abstract String getType();
    
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
               
        return "["+getType()+","
               +alertDateTime+","
               +eventUuid+","
               +alertDescription+"]";
    }

  
    static public Alert getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (Alert)ByteSerializable.getFromBytes(objBytes);
    }
}
