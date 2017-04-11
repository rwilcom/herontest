package com.nextcentury.herontest.bolts;

import com.nextcentury.herontest.HeronTestTupleSchema;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
        


/**
 * 
 * 
 * 
 * 
 * 
 */
public class PunchingBagBolt extends BaseRichBolt {

    private static final long serialVersionUID = -1L;
    private OutputCollector collector;
    private JedisPool jedisPool;
    
    private final Random randomizer = new Random();
    
    public static final String PUNCHINGBAG_NODE = "PunchingBagNode";
    public static final String PUNCHINGBAG_CHANNEL = "punchingBag";

    int myComponentTaskID;
    
    boolean ackNone = false;
    boolean ackFailAll = false;
    boolean ackFailSome = false;
    int failureRate = 10; /*%*/
    boolean leakMemory = false;
    int leakRate = 100000;
    boolean slowDown = false;
    int slowdownRateSec = 0;
    
    String memoryLeaker = new String();
    
    PunchMeSubscriber puchMeSubscriber;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
       myComponentTaskID = tc.getThisTaskId();
    
       Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                "PunchingBag component task ID:"+myComponentTaskID+"\n");

       collector = oc;

       jedisPool = new JedisPool(new JedisPoolConfig(), "10.10.83.58" ); //default port:6379 
       
       //need to subscribe on a new thread - 'subscibe' is a blokcking op
       new Thread(new Runnable() {
            @Override
            public void run() {
                try (Jedis jedis = jedisPool.getResource()) {       
                    puchMeSubscriber = new PunchMeSubscriber();       
                    jedis.subscribe(puchMeSubscriber, PUNCHINGBAG_CHANNEL);
                } catch (Exception any) {
                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.WARNING,
                        "problem subsrcibing to redis :"+any.toString()+"\n");
                }
            }
        }).start();       
    }
    
    @Override
    public void cleanup() {
        if( puchMeSubscriber!=null){
            puchMeSubscriber.unsubscribe();
        }
        if( jedisPool!=null){
            jedisPool.destroy();
        }
        
        super.cleanup(); //To change body of generated methods, choose Tools | Templates.        
    }
        
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
         declarer.declare(HeronTestTupleSchema.getBoltPayloadSchema());  
    }

    @Override
    public void execute(Tuple tuple) {

        Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                "execute - entering punching bag bolt\n");

        if( leakMemory ){
            leakMemory();
            
            Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                "PUNCHED - leaking memory; " + memoryLeaker.getBytes().length + " bytes.\n");
        }
                
        if( slowDown ){        
            Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                    "PUNCHED - slowing down process by "+slowdownRateSec+"s.\n");                        
            Utils.sleep(1000*slowdownRateSec); /*simulate slow down in this node*/            
        }

        if( ackFailAll ){
            collector.fail(tuple);
            
            Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                "PUNCHED - sending all failing ACKs\n");
            return;
        }

        if( ackFailSome ){
            if( randomizer.nextInt(100)< failureRate /*XX% random fail rate */ ){
                collector.fail(tuple);
                
                Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                    "PUNCHED - sending some failing ACKs\n");                
                return;
            }
        }
                
        collector.emit( tuple, tuple.getValues() );
        
        if( ackNone ){
            Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO, 
                    "PUNCHED - sending NO ACKs\n");            
        }else{
            collector.ack(tuple);
        }
        
       
    }

    private void leakMemory(){
        for( int i=0;  i<leakRate; i++){
            memoryLeaker+="This string is hogging memory! ";
        }
    }
    private void clearMemory(){
        memoryLeaker=new String();
    }    
    
    /**
     * 
     * 
     * 
     */    
    private class PunchMeSubscriber extends JedisPubSub {
        public void onMessage(String channel, String message) {
             Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                "PunchingBag received command '"+message+"' on channel "+channel+"\n");
             
            /* EXAMPLE COMMANDS (pub to Redis pub/sub channel 'punchingBag')             
                13.ackAll.null          //normal successful ACKs
                13.ackNone.null         //stop all ACKs
                13.ackFailAll.null      //send all Fail ACKs             
                13.ackFailSome.30       //send some Fail ACKs, 30% (the rest will be success ACKs)
                13.leakMemoryStart.1000 //start leaking memory, chunks of 1000s of sentences
                13.leakMemoryStop.null  //stop leaking memory
                13.clearMemory.null     //clear leaked memory (GC still needed)             
                13.slowDownStart.5      //slow down processing with 5 sec pause
                13.slowDownStop.null    //normal processing speed
            */
            
            if( !channel.equals(PUNCHINGBAG_CHANNEL)){
                return;//wrong channel received
            }
            
            try {
                /* container ID, command ID, command info */
                String[] messageElements = message.split("\\.");

                int taskID = Integer.parseInt(messageElements[0]);
                String commandID = messageElements[1];
                String commandInfo = messageElements[2];

                if( taskID != myComponentTaskID){
                    return; //wrong component
                }

                if(commandID.equalsIgnoreCase("ackAll") ){
                    ackNone = false;
                    ackFailAll = false;
                    ackFailSome = false;
                    
                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED ACK ALL activated.\n");                             
                }
                if(commandID.equalsIgnoreCase("ackNone") ){
                    ackNone = true;
                    ackFailAll = false;
                    ackFailSome = false;

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED NO ACK activated.\n");                             
                }
                if(commandID.equalsIgnoreCase("ackFailAll") ){
                    ackFailAll = true;
                    ackFailSome = false;
                    ackNone = false;

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED ACK FAIL activated.\n");                
                }
                if(commandID.equalsIgnoreCase("ackFailSome") ){
                    ackFailSome = true;
                    ackNone = false;
                    ackFailAll = false;
                    failureRate = Integer.parseInt(commandInfo);

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED ACK FAIL SOME activated - rate: "+failureRate+".\n");

                }
                if(commandID.equalsIgnoreCase("leakMemoryStart") ){
                    leakMemory = true;
                    leakRate = Integer.parseInt(commandInfo);

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED MEMORY LEAK activated - rate: "+leakRate+".\n");
                }
                if(commandID.equalsIgnoreCase("leakMemoryStop") ){
                    leakMemory = false;

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED MEMORY LEAK deactivated. Memory leaked to " + memoryLeaker.getBytes().length + " bytes.\n");                
                }
                if(commandID.equalsIgnoreCase("clearMemory") ){
                    clearMemory();

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED MEMORY CLEARED activated.\n");                                
                }
                if(commandID.equalsIgnoreCase("slowDownStart") ){
                    slowDown = true;
                    slowdownRateSec = Integer.parseInt(commandInfo);

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED SLOW DOWN activated - rate: "+slowdownRateSec+".\n");
                }
                if(commandID.equalsIgnoreCase("slowDownStop") ){
                    slowDown = false;
                    slowdownRateSec = 0;

                    Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.INFO,
                        "MESSAGERECEIVED SLOW DOWN deactivated - rate: "+slowdownRateSec+".\n");
                }
            }catch(Exception any){
               Logger.getLogger(PunchingBagBolt.class.getName()).log(Level.WARNING,
                        "PunchingBag message subscriber got message but ERROR occurred.\n");   
            }
            
        }

        public void onSubscribe(String channel, int subscribedChannels) {
        }

        public void onUnsubscribe(String channel, int subscribedChannels) {
        }

        public void onPSubscribe(String pattern, int subscribedChannels) {
        }

        public void onPUnsubscribe(String pattern, int subscribedChannels) {
        }

        public void onPMessage(String pattern, String channel, String message) {
        }
    }    
}    
    
