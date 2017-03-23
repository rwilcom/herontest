package com.nextcentury.herontest;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.Config;


import com.nextcentury.herontest.bolts.*;
import com.nextcentury.herontest.spouts.*;

import java.util.logging.Level;
import java.util.logging.Logger;
        


/**
 * 
 * DESIGN:
 * 
 *          create plane 'object' -> 
 *              located-activity / non-located-activity
 * 
 * normalizerBolt = normalize located-activity / non-located-activity
 * 
 * if located... 
 *      geoEnrichBolt = geo-enrichment, add hashes, etc
 *      geoAlertBolt = spatial alert processing ---> new SPOUT2 ... spatial alerts 
 * if non-located ...
 *      activityAlertBolt = activity alert processing ---> new SPOUT3 ... activity alerts
 * 
 * http://twitter.github.io/heron/api/
 * 
 */ 
public final class HeronTestTopology {

    private HeronTestTopology() {
    }

    public static long getMegabytes(long l){
         return l * 1024 * 1024;
    }
    public static long getGigabytes(long l){
         return l * 1024 * 1024 * 1024;
    }  
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 1) {
            throw new RuntimeException("Specify topology name");
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(AirTrafficSpout.AIRTRAFFIC_DATA_SOURCE, new AirTrafficSpout(), 1);

        //normalize data into location and activity objects
        //then route that data to the appropriate streams
        builder.setBolt(NormalizerBolt.NORMALIZER_NODE, new NormalizerBolt(), 1)
                .shuffleGrouping(AirTrafficSpout.AIRTRAFFIC_DATA_SOURCE);        
        builder.setBolt(RouterBolt.ROUTER_NODE, new RouterBolt(), 1)
                .shuffleGrouping(NormalizerBolt.NORMALIZER_NODE);        

        //receive location objects and enrich them (if possible); 
        //send enriched location objects to the location alerting stream
        builder.setBolt(LocationEnrichBolt.LOCATION_ENRICH_NODE, new LocationEnrichBolt(), 1)
                .shuffleGrouping(RouterBolt.ROUTER_NODE,RouterBolt.LOCATION_ROUTER_STREAM);          
        builder.setBolt(LocationAlertBolt.LOCATION_ALERT_NODE, new LocationAlertBolt(), 1)
                .shuffleGrouping(LocationEnrichBolt.LOCATION_ENRICH_NODE); 

        //receive activity objects and enrich them (if possible); 
        //send enriched activity objects to the activity alerting stream
        builder.setBolt(ActivityEnrichBolt.ACTIVITY_ENRICH_NODE, new ActivityEnrichBolt(), 1)
                .shuffleGrouping(RouterBolt.ROUTER_NODE,RouterBolt.ACTIVITY_ROUTER_STREAM);          
        builder.setBolt(ActivityAlertBolt.ACTIVITY_ALERT_NODE, new ActivityAlertBolt(), 1)
                .shuffleGrouping(ActivityEnrichBolt.ACTIVITY_ENRICH_NODE); 
                
        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxSpoutPending(1000 * 1000 * 1000);//large number to prevent a max
        conf.setEnableAcking(true);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
        conf.setNumStmgrs(1); //number of stream managers        
        conf.setComponentRam(AirTrafficSpout.AIRTRAFFIC_DATA_SOURCE, getMegabytes(500) );  
        conf.setComponentRam(NormalizerBolt.NORMALIZER_NODE, getMegabytes(200) ); 
        conf.setComponentRam(RouterBolt.ROUTER_NODE, getMegabytes(200) );
        conf.setComponentRam(LocationEnrichBolt.LOCATION_ENRICH_NODE, getMegabytes(200) ); 
        conf.setComponentRam(ActivityEnrichBolt.ACTIVITY_ENRICH_NODE, getMegabytes(200) ); 
        conf.setComponentRam(LocationAlertBolt.LOCATION_ALERT_NODE, getMegabytes(200) ); 
        conf.setComponentRam(ActivityAlertBolt.ACTIVITY_ALERT_NODE, getMegabytes(200) ); 

        conf.setContainerDiskRequested( getGigabytes(1) ); 
        conf.setContainerCpuRequested( 2 );
    
        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }      
   
}
