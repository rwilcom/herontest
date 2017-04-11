package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;
import com.nextcentury.herontest.dto.Alert;
import com.nextcentury.herontest.dto.Location;
import com.nextcentury.herontest.dto.LocationAlert;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
        


/**
 * 
 * 
 * 
 * 
 * 
 */
public class LocationAlertBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
    
    private final Random randomizer = new Random();
    
    public static final String LOCATION_ALERT_NODE = "LocationAlertNode";
    public static final String LOCATION_ALERT_STREAM = LOCATION_ALERT_NODE+".AlertStream";
            
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
       collector = oc;
    }
    
    @Override
    public void cleanup() {
        
        super.cleanup(); //To change body of generated methods, choose Tools | Templates.        
    }
        
  
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(HeronTestTupleSchema.getBoltAlertSchema());   
        declarer.declareStream(LOCATION_ALERT_STREAM, HeronTestTupleSchema.getBoltAlertSchema());
    }

    @Override
    public void execute(Tuple tuple) {
        
        String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
        
        if(!payloadClass.equalsIgnoreCase(Location.class.getName())){
            Logger.getLogger(LocationAlertBolt.class.getName()).log(Level.SEVERE, 
                "execute - unexpected class in payload: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }

        String locationGeoHash = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_GEOHASH);
        
        //TODO - test the geo hash for alerts
        
        //TODO - if UUID was passed along in the tuple then we wouldn't have
        //       to rebuild the object - that would likely make this faster

        //proving rehydration here from bytes
        Location location;
        byte[] payloadAsBytes = tuple.getBinaryByField(HeronTestTupleSchema.SCHEMA_PAYLOAD_AS_BYTES);
        try{
            location = Location.getFromBytes(payloadAsBytes);
            //Logger.getLogger(LocationAlertBolt.class.getName()).log(Level.INFO, 
            //    "execute - rehydrated payload: "+location.toString()+"\n");
        }catch( Exception any ){
            Logger.getLogger(LocationAlertBolt.class.getName()).log(Level.SEVERE, 
                "execute - rehydration failed: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }    
         
       if( randomizer.nextInt(100)<10 /*10% random - alert happened*/ ){
            
            //lame alert example
            String flightStatus = location.objectId + " is at "
                    +location.latitude+","+location.longitude
                    +" heading "+location.headingDecDegFromNorth0
                    +"("+locationGeoHash+")";
            
            LocationAlert locationAlert = new LocationAlert();
            locationAlert.alertDateTime = new Date(System.currentTimeMillis());
            locationAlert.eventUuid = location.uuid; //TODO - faster to pass this along in the tuple!
            locationAlert.alertDescription =  "LOACTION ALERT: "+location.uuid +" : "+flightStatus;
                    

            //recreate the bytes (with the enrichment)
            byte[] payloadAsBytesLocationAlert=null;
            try {
                payloadAsBytesLocationAlert = locationAlert.getAsBytes();           
            }catch(IOException ioe){
                Logger.getLogger(LocationAlertBolt.class.getName()).log(Level.SEVERE,
                    "execute - location alert object as bytes failed: "+ioe.toString()+"\n");
                collector.fail(tuple);            
            }            
            
            GlobalMetrics.incr("location_alerted");

            //emit the new tuple (the enriched version)
            collector.emit( LOCATION_ALERT_STREAM,
                            tuple,
                            new Values( locationAlert.eventUuid, 
                                locationAlert.alertDescription, 
                                Alert.class.getName(), 
                                payloadAsBytesLocationAlert));                    
            
            Logger.getLogger(LocationAlertBolt.class.getName()).log(Level.INFO, 
                "execute - location alert forwarding : "+locationAlert.toString()+"\n");              
        }

        collector.ack(tuple);
    }

}    
    
