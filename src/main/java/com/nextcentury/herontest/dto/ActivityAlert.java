package com.nextcentury.herontest.dto;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public class ActivityAlert extends ByteSerializable {

    public ActivityAlert(){}

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
               
        return "["+"ACTIVITY_ALERT"+","
               +alertDateTime+","
               +eventUuid+","
               +alertDescription+"]";
    }
    
    static public ActivityAlert getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (ActivityAlert)ByteSerializable.getFromBytes(objBytes);
    }
}
