package com.nextcentury.herontest.dto;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public class Location extends ByteSerializable {

    public Location(){}
    
    public String uuid;
    
    public Date eventDateTime;    
    public String objectId;
    public String originCountry;
    public Double longitude;
    public Double latitude;
    public Double altitudeMeters;
    public Double headingDecDegFromNorth0;
    public String geoHash;

    @Override
    public String toString(){
        return "uuid:"+uuid+
               ";eventDateTime:"+eventDateTime+
               ";objectId:"+objectId+
               ";originCountry:"+originCountry+              
               ";longitude:"+longitude+
               ";latitude:"+latitude+
               ";altitudeMeters:"+altitudeMeters+
               ";headingDecDegFromNorth0:"+headingDecDegFromNorth0+
               ";geoHash:"+geoHash;
    }
    
    /**
     * 
     * @return 
     */    
    public String toStringTuple    (){
               
        return "["+"LOCATION"+","
               +uuid+","
               +eventDateTime+","
               +objectId+","
               +originCountry+","
               +longitude+","
               +latitude+","
               +altitudeMeters+","
               +headingDecDegFromNorth0+","
               +geoHash +"]";
    }
    
    static public Location getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (Location)ByteSerializable.getFromBytes(objBytes);
    }
}
