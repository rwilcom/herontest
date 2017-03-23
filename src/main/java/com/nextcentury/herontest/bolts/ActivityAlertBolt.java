package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;
import com.nextcentury.herontest.dto.Activity;
import com.nextcentury.herontest.dto.ActivityAlert;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Tuple;
import java.util.Date;

import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


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
    private JedisPool jedisPool;
    
    private Random randomizer = new Random();

    
    public static String ACTIVITY_ALERT_NODE = "ActivityAlertNode";
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
        
        jedisPool = new JedisPool(new JedisPoolConfig(), "10.10.83.58" ); //default port:6379
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
         //nothing ot declare - not forwarding on to other bolts
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
            
            GlobalMetrics.incr("activity_alerted");
            
            //send alert to common pub/sub queue (Kafka?)
            publishActivityAlert( activityAlert );
            
            Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.INFO,
                "execute - activity alert firing  : "+activity.toString()+"\n");                
        }
        
        //we are DONE with the original
        //tuple since we've processed the alert
        // -- end of stream
        collector.ack(tuple);
    }

    
    /**
     * 
     * @param activityAlert
     */
    private void publishActivityAlert( ActivityAlert activityAlert ){
        
        try (Jedis jedis = jedisPool.getResource()) {
            
            jedis.publish("activityAlerts", activityAlert.toStringTuple()); 
            
        }catch(Exception any){
            Logger.getLogger(ActivityAlertBolt.class.getName()).log(Level.WARNING,
                "problem publishing activity alert "+activityAlert.eventUuid+": "+any.toString()+"\n");
        }
    }
    
}        

