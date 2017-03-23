package com.nextcentury.herontest.dto;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public class Activity extends ByteSerializable {

    public Activity(){}

    public String uuid;
    
    public Date eventDateTime;
    public String objectId;
    public Boolean onGround;
    public Double velocityMetersPerSec;
    public Double verticalRateMetersPerSec;

    @Override
    public String toString(){
        return "uuid:"+uuid+
               ";eventDateTime:"+eventDateTime+
               ";timeOfVelocityDateTime:"+eventDateTime+
               ";objectId:"+objectId+
               ";onGround:"+onGround+
               ";velocityMetersPerSec:"+velocityMetersPerSec+
               ";veticalRateMetersPerSec:"+verticalRateMetersPerSec;
    }
    
    /**
     * 
     * @return 
     */    
    public String toStringTuple    (){
               
        return "["+"ACTIVITY"+","
               +uuid+","
               +eventDateTime+","
               +objectId+","
               +onGround+","
               +velocityMetersPerSec+","
               +verticalRateMetersPerSec +"]";
    }    
    
    static public Activity getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (Activity)ByteSerializable.getFromBytes(objBytes);
    }
}
