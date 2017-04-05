package com.nextcentury.herontest;

import com.twitter.heron.api.tuple.Fields;

/**
 * 
  * 
 */ 
public final class HeronTestTupleSchema {

    public static final String SCHEMA_UUID = "uuId";
    public static final String SCHEMA_OBJECTID = "objectId";
    public static final String SCHEMA_ORIGINCOUNTRY = "originCountry";
    public static final String SCHEMA_ISFLYING = "isFlying";
    public static final String SCHEMA_PAYLOADCLASS = "payloadClass";
    public static final String SCHEMA_PAYLOAD_AS_BYTES = "payloadAsBytes";
    public static final String SCHEMA_GEOHASH = "geoHash";
    public static final String SCHEMA_ALERT_DESCRIPTION = "alertDescription";
    
    
    private HeronTestTupleSchema() {
    }
        public static Fields getSpoutSchema(){
        return new Fields(
                SCHEMA_OBJECTID, 
                SCHEMA_ORIGINCOUNTRY, 
                SCHEMA_ISFLYING, 
                SCHEMA_PAYLOADCLASS, 
                SCHEMA_PAYLOAD_AS_BYTES);
    }
    
    public static Fields getBoltPayloadSchema(){
        return new Fields(
                SCHEMA_OBJECTID,                 
                SCHEMA_PAYLOADCLASS,
                SCHEMA_PAYLOAD_AS_BYTES);
    }    
    
    
    public static Fields getBoltEnrichedLocationSchema(){
        return new Fields(
                SCHEMA_OBJECTID,                 
                SCHEMA_GEOHASH,
                SCHEMA_PAYLOADCLASS,
                SCHEMA_PAYLOAD_AS_BYTES);
    }    
    
    public static Fields getBoltAlertSchema(){
        return new Fields(
                SCHEMA_UUID,                 
                SCHEMA_ALERT_DESCRIPTION,
                SCHEMA_PAYLOADCLASS,
                SCHEMA_PAYLOAD_AS_BYTES);
    }       
}
