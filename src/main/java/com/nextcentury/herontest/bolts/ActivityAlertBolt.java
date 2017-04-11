package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;
import com.nextcentury.herontest.dto.Activity;
import com.nextcentury.herontest.dto.ActivityAlert;
import com.nextcentury.herontest.dto.Alert;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import java.io.IOException;
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
public class ActivityAlertBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
    
    private final Random randomizer = new Random();

    
    public static final String ACTIVITY_ALERT_NODE = "ActivityAlertNode";
    public static final String ACTIVITY_ALERT_STREAM = ACTIVITY_ALERT_NODE+".AlertStream";
    
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
        declarer.declareStream(ACTIVITY_ALERT_STREAM, HeronTestTupleSchema.getBoltAlertSchema());

    }
   

    @Override
    public void execute(Tuple tuple) {
        
        String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
        
        if(!payloadClass.equalsIgnoreCase(Activity.class.getName())){
            Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.SEVERE,
                "execute - unexpected class in payload: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }

        //proving rehydration here from bytes
        Activity activity;
        byte[] payloadAsBytes = tuple.getBinaryByField(HeronTestTupleSchema.SCHEMA_PAYLOAD_AS_BYTES);
        try{
            activity = Activity.getFromBytes(payloadAsBytes);
            //Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.INFO,
            //    "execute - rehydrated payload: "+activity.toString()+"\n");
        }catch( Exception any ){
            Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.SEVERE,
                "execute - rehydration failed: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
            
        if( randomizer.nextInt(100)<10 /*10% random - alert happened*/ ){
         
            //lame alert example
            String flightStatus = activity.objectId + " is on the ground";
            if(!activity.onGround){
                flightStatus = activity.objectId + " is in the air travelling at "+activity.velocityMetersPerSec +"mps";
            }
            ActivityAlert activityAlert = new ActivityAlert();
            activityAlert.alertDateTime = new Date(System.currentTimeMillis());
            activityAlert.eventUuid = activity.uuid; //TODO - faster to pass this along in the tuple!
            activityAlert.alertDescription = "ACTIVITY ALERT: "+activity.uuid +" : "+flightStatus;
            
            //recreate the bytes (with the enrichment)
            byte[] payloadAsBytesActivityAlert=null;
            try {
                payloadAsBytesActivityAlert = activityAlert.getAsBytes();           
            }catch(IOException ioe){
                Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.SEVERE,
                    "execute - activity alert object as bytes failed: "+ioe.toString()+"\n");
                collector.fail(tuple);            
            }                      
            
            GlobalMetrics.incr("activity_alerted");
                              
            //emit the new tuple (the enriched version)
            collector.emit( ACTIVITY_ALERT_STREAM,
                            tuple,
                            new Values( activityAlert.eventUuid, 
                                activityAlert.alertDescription, 
                                Alert.class.getName(), 
                                payloadAsBytesActivityAlert));                       
            
            Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.INFO,
                "execute - activity alert forwarding  : "+activity.toString()+"\n");                
        }
        

        collector.ack(tuple);
    }
    
}        

