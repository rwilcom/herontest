package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;

import com.nextcentury.herontest.dto.Activity;
import com.nextcentury.herontest.dto.Location;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

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
public class RouterBolt extends BaseRichBolt {

    public static final String ROUTER_NODE = "RouterNode";
    
    public static final String LOCATION_ROUTER_STREAM = ROUTER_NODE+".LocationStream";
    public static final String ACTIVITY_ROUTER_STREAM = ROUTER_NODE+"ActivityStream";
    
    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
   
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declareStream(LOCATION_ROUTER_STREAM, HeronTestTupleSchema.getBoltPayloadSchema());
        declarer.declareStream(ACTIVITY_ROUTER_STREAM, HeronTestTupleSchema.getBoltPayloadSchema());        
        
    }
    
    @Override
    public void execute(Tuple tuple) {
       
       String payloadClass = tuple.getStringByField(HeronTestTupleSchema.SCHEMA_PAYLOADCLASS);
       String streamName = null;
       if( payloadClass.equalsIgnoreCase(Activity.class.getName())){
           streamName = ACTIVITY_ROUTER_STREAM;
       }else if(payloadClass.equalsIgnoreCase(Location.class.getName())){          
           streamName = LOCATION_ROUTER_STREAM;
       }else{
           Logger.getLogger(RouterBolt.class.getName()).log(Level.SEVERE, 
                "execute - invalid tuple passed\n");
           collector.fail(tuple);
           return;
       }
       
       collector.emit(streamName, tuple, tuple.getValues());
       
       //do not ack here we want the new tuples to 
       //be tracked through to the subsequent bolts
    }

}
