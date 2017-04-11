package com.nextcentury.herontest.spouts;

import com.nextcentury.herontest.HeronTestTupleSchema;
import com.twitter.heron.api.metric.GlobalMetrics;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.spout.SpoutOutputCollector;

import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import com.nextcentury.opensky.OpenSky;
import com.nextcentury.opensky.OpenSkyRawAirTraffic;
import java.io.IOException;
import java.util.ArrayList;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
        


 
/**
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 */
public class AirTrafficSpout extends BaseRichSpout {

    public static String AIRTRAFFIC_DATA_SOURCE = "AirTrafficDataSource";
    
    private static final long serialVersionUID = -1L;
    private SpoutOutputCollector collector;

    ArrayList<OpenSkyRawAirTraffic> osraList;
    int totalInBatch;
    int indexOnBatch;

    public AirTrafficSpout() {}

    @Override
    public void open(Map<String, Object> map,
            TopologyContext tc,
            SpoutOutputCollector soc) {
        collector = soc;

        readNextAirlineTrafficBatch();
    }

    public void close() {
    }

    /**
     * 
     * 
     * 
     */
    public void readNextAirlineTrafficBatch(){

        //nothing to do yet
        if( osraList!=null ){
            if( indexOnBatch<osraList.size() ){
                return;
            }      
        
            Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "... pausing before reading more\n");            
            Utils.sleep(10*1000); // pause - don't want to hammer the service!
        }//else, has not been read yet

        Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "reading new batch of airtraffic data\n");
        
        osraList = OpenSky.readAirTraffic();       
        indexOnBatch=0;
        
        Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "new airtraffic data read: "+osraList.size()+"\n");
    }

    /**
     * 
     * 
     * 
     * @return 
     */
    public OpenSkyRawAirTraffic getNextAirLineTrafficRecord(){
        return osraList.get(indexOnBatch++);
    }

    /**
     * 
     * 
     * 
     */
    public void nextTuple() {
        // explicitly slow down the spout to avoid the stream mgr to be the bottleneck
        Utils.sleep(1);

        readNextAirlineTrafficBatch(); //may not read anything

        final OpenSkyRawAirTraffic osra = getNextAirLineTrafficRecord();

        String callsign = osra.callsign;
        String originCountry = osra.originCountry;
        Boolean isFlying = !osra.onGround;
        String payloadClass = OpenSkyRawAirTraffic.class.getName();            
        byte[] payloadAsBytes;
        try {
            payloadAsBytes = osra.getAsBytes();                

            String messageUUID = osra.icao24TransponderAddr+"."+System.currentTimeMillis();

            
            Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "nextTuple - pushing air traffic for "+osra.callsign+"....messageID="+messageUUID+"\n");
            
            collector.emit(
                    new Values( callsign, 
                                originCountry, 
                                isFlying, 
                                payloadClass, 
                                payloadAsBytes), 
                    messageUUID /*ack/fail message ID*/);            
        } catch (IOException ex) {
            Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.SEVERE, ex.getMessage());
            
            //TODO send failure here
        }
    }

    public void ack(Object msgId) {
        Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "ack: "+msgId+"\n");        
    }

    public void fail(Object msgId) {
        Logger.getLogger(AirTrafficSpout.class.getName()).log(Level.INFO, 
                "fail: "+msgId+"\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( HeronTestTupleSchema.getSpoutSchema() );
    }

}

