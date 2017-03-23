package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;
import com.nextcentury.herontest.dto.Activity;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.util.Map;
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
public class ActivityEnrichBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
    private JedisPool jedisPool;
    
    public static String ACTIVITY_ENRICH_NODE = "ActivityEnrichNode";
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
        
        jedisPool = new JedisPool(new JedisPoolConfig(), "10.10.83.58" ); //default port:6379
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(HeronTestTupleSchema.getBoltPayloadSchema());   
    }    

    @Override
    public void execute(Tuple tuple) {
        
        String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
        
        //Logger.getLogger(ActivityEnrichBolt.class.getName()).log(Level.INFO,
        //        "passing data through\n");
    
        if(!payloadClass.equalsIgnoreCase(Activity.class.getName())){
            Logger.getLogger(ActivityEnrichBolt.class.getName()).log(Level.SEVERE,
                "execute - unexpected class in payload: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
        
        //proving rehydration here from bytes
        Activity activity;
        byte[] payloadAsBytes = tuple.getBinaryByField(HeronTestTupleSchema.SCHEMA_PAYLOAD_AS_BYTES);
        try{
            activity = Activity.getFromBytes(payloadAsBytes);
            //Logger.getLogger(ActivityEnrichBolt.class.getName()).log(Level.INFO,
            //    "execute - rehydrated payload: "+activity.toString()+"\n");
        }catch( Exception any ){
            Logger.getLogger(ActivityEnrichBolt.class.getName()).log(Level.SEVERE,  
                "execute - rehydration failed: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
        
        //save off a history of the raw activity 
        publishActivity( activity );
        
        //pass on to next bolt
        collector.emit( tuple, tuple.getValues() );
               
       //do not ack here we want the new tuples to 
       //be tracked through to the subsequent bolts
    }
    
    /**
     * 
     * @param activity 
     */
    private void publishActivity( Activity activity ){
        
        try (Jedis jedis = jedisPool.getResource()) {
            
            jedis.publish("activity", activity.toStringTuple()); 
            
        }catch(Exception any){
            Logger.getLogger(ActivityEnrichBolt.class.getName()).log(Level.WARNING,
                "problem publishing activity "+activity.uuid+": "+any.toString()+"\n");
        }
    }

    

}        

