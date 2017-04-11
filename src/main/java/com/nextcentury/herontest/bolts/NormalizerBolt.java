package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;

import com.nextcentury.herontest.dto.Activity;
import com.nextcentury.herontest.dto.Location;
import com.nextcentury.opensky.OpenSkyRawAirTraffic;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.tuple.Fields;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;
import java.io.IOException;
import java.util.Date;


import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
        



/**
 * 
 * 
 * 
 * 
 * 
 */
public class NormalizerBolt extends BaseRichBolt {

    public static final String NORMALIZER_NODE = "NormalizerNode";
    
    private static final long serialVersionUID = -1L;
    private OutputCollector collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(HeronTestTupleSchema.getBoltPayloadSchema());   
    }    
    
    @Override
    public void execute(Tuple tuple) {
        String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
        
        if(!payloadClass.equalsIgnoreCase(OpenSkyRawAirTraffic.class.getName())){
            Logger.getLogger(NormalizerBolt.class.getName()).log(Level.SEVERE, 
                "execute - unexpected class in payload: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
        
        //proving rehydration here rom bytes
        OpenSkyRawAirTraffic osra;
        byte[] payloadAsBytes = tuple.getBinaryByField(HeronTestTupleSchema.SCHEMA_PAYLOAD_AS_BYTES);        
        try{
            osra = OpenSkyRawAirTraffic.getFromBytes(payloadAsBytes);
            //Logger.getLogger(NormalizerBolt.class.getName()).log(Level.INFO,
            //    "execute - rehydrated payload: "+osra.callsign+"\n");
        }catch( Exception any ){
            Logger.getLogger(NormalizerBolt.class.getName()).log(Level.SEVERE, 
                "execute - rehydration failed: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
        
        
        GlobalMetrics.incr("raw_ingested");
        
        //this 'normalizer' will read the raw feed and extract
        //here we will lose the raw object and produce two new
        //application specific objects from the raw data:
        //  location - a location object (derived from a located event)
        //  activity - an activity object (derived from a located or non located event)

        //----activity
        Activity activity = new Activity();
        activity.uuid = "ACT"+System.currentTimeMillis()+osra.callsign; //not a perfect uuid
        activity.eventDateTime = new Date(System.currentTimeMillis());
        activity.objectId = osra.callsign;
        activity.onGround = osra.onGround;
        activity.velocityMetersPerSec = osra.velocityMetersPerSec;
        activity.verticalRateMetersPerSec = osra.verticalRateMetersPerSec;
                
        byte[] payloadAsBytesActivity=null;
        try {
            payloadAsBytesActivity = activity.getAsBytes();           
        }catch(IOException ioe){
            Logger.getLogger(NormalizerBolt.class.getName()).log(Level.SEVERE,
                "execute - activity object as bytes failed: "+ioe.toString()+"\n");
            collector.fail(tuple);            
        }
        GlobalMetrics.incr("activity_generated");
 
        //----location
        Location location = new Location();
        location.uuid = "LOC"+System.currentTimeMillis()+osra.callsign;; //not a perfect uuid
        location.eventDateTime = new Date(System.currentTimeMillis());
        location.altitudeMeters = osra.altitudeMeters;
        location.headingDecDegFromNorth0 = osra.headingDecDegFromNorth0;
        location.originCountry = osra.originCountry;
        location.latitude = osra.latitude;
        location.longitude = osra.longitude;
          
        byte[] payloadAsBytesLocation=null;
        try {
            payloadAsBytesLocation = location.getAsBytes();           
        }catch(IOException ioe){
            Logger.getLogger(NormalizerBolt.class.getName()).log(Level.SEVERE, 
                 "execute - location object as bytes failed: "+ioe.toString()+"\n");
            collector.fail(tuple);            
        }
        GlobalMetrics.incr("location_generated");

        //emit the resulting activity
        collector.emit( tuple, new Values( activity.objectId,                                 
                                Activity.class.getName(), 
                                payloadAsBytesActivity));

        //emit the resulting location        
        collector.emit( tuple, new Values( location.objectId,                                 
                                Location.class.getName(), 
                                payloadAsBytesLocation));
                        
        //we are DONE with the original
        //object here since we've tranformed 
        //it into two other object types
        //so ACK the results
        collector.ack(tuple);

    }

}    
