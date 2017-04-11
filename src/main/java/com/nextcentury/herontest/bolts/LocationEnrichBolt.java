package com.nextcentury.herontest.bolts;


import com.nextcentury.herontest.HeronTestTupleSchema;
import com.nextcentury.herontest.dto.Location;

import com.twitter.heron.api.metric.GlobalMetrics;
import com.github.davidmoten.geo.GeoHash;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import java.io.IOException;

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
public class LocationEnrichBolt extends BaseRichBolt {


    public static final String LOCATION_ENRICH_NODE = "LocationEnrichNode";
            
    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
    private JedisPool jedisPool;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
        jedisPool = new JedisPool(new JedisPoolConfig(), "10.10.83.58" ); //default port:6379
    }

    @Override
    public void cleanup() {
        
        if( jedisPool!=null){
            jedisPool.destroy();
        }
        
        super.cleanup(); //To change body of generated methods, choose Tools | Templates.        
    }
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(HeronTestTupleSchema.getBoltEnrichedLocationSchema());    
    }    

    @Override
    public void execute(Tuple tuple) {
        
        String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
        
        if(!payloadClass.equalsIgnoreCase(Location.class.getName())){
            Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.SEVERE,
                "execute - unexpected class in payload: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
        
        //proving rehydration here from bytes
        Location location;
        byte[] payloadAsBytes = tuple.getBinaryByField(HeronTestTupleSchema.SCHEMA_PAYLOAD_AS_BYTES);
        try{
            location = Location.getFromBytes(payloadAsBytes);
            //Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.INFO,
            //    "execute - rehydrated payload: "+location.toString()+"\n");
        }catch( Exception any ){
            Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.SEVERE,  
                "execute - rehydration failed: "+payloadClass+"\n");
            collector.fail(tuple);
            return;
        }
                
        //enrich the location with extra information (for example: apply a geohash)
        if( location.latitude!=null && location.longitude!=null){
            location.geoHash = GeoHash.encodeHash(location.latitude, location.longitude);
        }else{
            Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.SEVERE,  
                "execute - could not fine lat/lon on class: "+location.uuid+"\n");
            
            //this is a "soft" error situation 
            //so do not send back a "fail" ack           
            
            return; //throw this location out
        }

        Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.INFO,
                "execute - location object enriched: "+location.toString()+"\n");        
        
        //save off a history of the raw activity 
        publishLocation( location );

        //recreate the bytes (with the enrichment)
        byte[] payloadAsBytesLocationEnriched=null;
        try {
            payloadAsBytesLocationEnriched = location.getAsBytes();           
        }catch(IOException ioe){
            Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.SEVERE,
                "execute - location object as bytes failed: "+ioe.toString()+"\n");
            collector.fail(tuple);            
        }
        
        GlobalMetrics.incr("location_enriched");

        //emit the new tuple (the enriched version)
        collector.emit( tuple,
                        new Values( location.objectId, 
                                location.geoHash, //note: additional tuple value in this enrichment
                                Location.class.getName(), 
                                payloadAsBytesLocationEnriched));
               
        //done with original location tuple (immutable)
        // - passing on newly enriched tuple
        collector.ack(tuple);
        
    }
    
    /**
     * 
     * @param location 
     */
    private void publishLocation( Location location ){
        
        try (Jedis jedis = jedisPool.getResource()) {
            
            jedis.publish("locations", location.toStringTuple()); 
            
        }catch(Exception any){
            Logger.getLogger(LocationEnrichBolt.class.getName()).log(Level.WARNING,
                "problem publishing location "+location.uuid+": "+any.toString()+"\n");
        }
    }

    
    /**
     * TESTING CODE HERE     
     */
    /*
    public static void main(String[] args) throws Exception {
        
        JedisPool pool = 
                new JedisPool(new JedisPoolConfig(), "10.10.83.58" ); //default port:6379
        Jedis jedis = pool.getResource();
        try {
            //jedis.set("heronTEST", "this_is_a_test");
            //String value = jedis.get("heronTEST");
            //System.out.print("FOUND VALUE: "+value+"\n");
            //jedis.del("heronTEST");
            jedis.publish("testPUBSUB", "TEST12345 some tuple of data");
            
        }catch(Exception any){
                System.out.print("ERROR: "+any.getMessage()+"\n");
        }finally{
            jedis.close();
        }
        
        pool.destroy();
    }*/
    
     

}
