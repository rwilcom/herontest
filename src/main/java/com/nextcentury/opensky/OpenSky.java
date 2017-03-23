/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nextcentury.opensky;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

/**
 *
 * Raw airplane traffic feed via REST API: 
 *     https://opensky-network.org/apidoc/
 *     https://opensky-network.org/apidoc/rest.html
 *     https://opensky-network.org/api/states/all  (the feed!)
 * 
 * 
 * @author rwilcom
 */
public class OpenSky {
    
    private static final Client jClient;
    static{
        jClient = hostIgnoringClient(); //create();
        jClient.setConnectTimeout(30000);
        jClient.setReadTimeout(30000);
    }
    
    static public Client hostIgnoringClient() {
        try
            { 
                TrustManager[] trustAllCerts = new TrustManager[]{ new X509TrustManager(){
                        public X509Certificate[] getAcceptedIssuers(){return null;}
                        public void checkClientTrusted(X509Certificate[] certs, String authType){}
                        public void checkServerTrusted(X509Certificate[] certs, String authType){}
                    }
                };
            
            SSLContext sslcontext = SSLContext.getInstance( "TLS" );
            sslcontext.init( null, trustAllCerts, new SecureRandom() );
            DefaultClientConfig config = new DefaultClientConfig();
            Map<String, Object> properties = config.getProperties();
            HTTPSProperties httpsProperties = new HTTPSProperties(
                    new HostnameVerifier() {
                        @Override
                        public boolean verify( String s, SSLSession sslSession ) {
                            return true;
                        }
                    }, sslcontext
            );
            properties.put( HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, httpsProperties );
            return Client.create( config );
        }
        catch ( KeyManagementException | NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }
    }    
    
    
    /**
     * 
     * 
     * 
     * 
     */    
    static public ArrayList<OpenSkyRawAirTraffic> readAirTraffic(){

        
        ArrayList<OpenSkyRawAirTraffic> openSkyRawList = 
                new ArrayList<>();

        final WebResource wr = jClient.resource("https://opensky-network.org/api/states/all");
        WebResource.Builder builder = wr.getRequestBuilder();
        builder.accept(MediaType.APPLICATION_JSON);
        ClientResponse resp = builder.get(ClientResponse.class);
        final int status = resp.getStatus();
        if( status>300) {
            //unexpected error
        }else{
            String jsonBody = resp.getEntity(String.class);
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonOpenSkyObj = jsonParser.parse(jsonBody).getAsJsonObject();

            Long timeAsLong = jsonOpenSkyObj.get("time").getAsLong();

            if( jsonOpenSkyObj.get("states").isJsonNull()){
                return openSkyRawList; // empty
            }
            
            JsonArray jsonOpenSkyStatesListArr = jsonOpenSkyObj.getAsJsonArray("states");
            for( JsonElement openSkyState : jsonOpenSkyStatesListArr){
                /*                    
                0 	icao24 	string 	Unique ICAO 24-bit address of the transponder in hex string representation.
                1 	callsign 	string 	Callsign of the vehicle (8 chars). Can be null if no callsign has been received.
                2 	origin_country 	string 	Country name inferred from the ICAO 24-bit address.
                3 	time_position 	float 	Unix timestamp (seconds) for the last position update. Can be null if no position report was received by OpenSky within the past 15s.
                4 	time_velocity 	float 	Unix timestamp (seconds) for the last velocity update. Can be null if no velocity report was received by OpenSky within the past 15s.
                5 	longitude 	float 	WGS-84 longitude in decimal degrees. Can be null.
                6 	latitude 	float 	WGS-84 latitude in decimal degrees. Can be null.
                7 	altitude 	float 	Barometric or geometric altitude in meters. Can be null.
                8 	on_ground 	boolean 	Boolean value which indicates if the position was retrieved from a surface position report.
                9 	velocity 	float 	Velocity over ground in m/s. Can be null.
                10 	heading 	float 	Heading in decimal degrees clockwise from north (i.e. north=0°). Can be null.
                11 	vertical_rate 	float 	Vertical rate in m/s. A positive value indicates that the airplane is climbing, a negative value indicates that it descends. Can be null.
                12 	sensors 	int[]   (will be null for 'all' call)
                */
                
                OpenSkyRawAirTraffic openSkyRaw = new OpenSkyRawAirTraffic(); 
                openSkyRaw.lastReadDateTime = new Date(timeAsLong*1000);  

                JsonArray jsonOpenSkyStateArr = openSkyState.getAsJsonArray();

                if( !jsonOpenSkyStateArr.get(0).isJsonNull())                    
                    openSkyRaw.icao24TransponderAddr = jsonOpenSkyStateArr.get(0).getAsString().trim();

                if( !jsonOpenSkyStateArr.get(1).isJsonNull())                    
                    openSkyRaw.callsign = jsonOpenSkyStateArr.get(1).getAsString().trim();

                if( !jsonOpenSkyStateArr.get(2).isJsonNull())                    
                    openSkyRaw.originCountry = jsonOpenSkyStateArr.get(2).getAsString().trim();

                if( !jsonOpenSkyStateArr.get(3).isJsonNull()){
                    Long timeOfPosition = jsonOpenSkyStateArr.get(3).getAsLong();
                    openSkyRaw.timeOfPositionDateTime = new Date(timeOfPosition*1000);
                }

                if( !jsonOpenSkyStateArr.get(4).isJsonNull()){
                   Long timeOfVelocity = jsonOpenSkyStateArr.get(4).getAsLong();
                   openSkyRaw.timeOfVelocityDateTime = new Date(timeOfVelocity*1000);
                }

                if( !jsonOpenSkyStateArr.get(5).isJsonNull())                    
                    openSkyRaw.longitude = jsonOpenSkyStateArr.get(5).getAsDouble();

                if( !jsonOpenSkyStateArr.get(6).isJsonNull())                    
                    openSkyRaw.latitude = jsonOpenSkyStateArr.get(6).getAsDouble();

                if( !jsonOpenSkyStateArr.get(7).isJsonNull())                    
                    openSkyRaw.altitudeMeters = jsonOpenSkyStateArr.get(7).getAsDouble();

                if( !jsonOpenSkyStateArr.get(8).isJsonNull())                    
                    openSkyRaw.onGround = jsonOpenSkyStateArr.get(8).getAsBoolean();

                if( !jsonOpenSkyStateArr.get(9).isJsonNull())                    
                    openSkyRaw.velocityMetersPerSec = jsonOpenSkyStateArr.get(9).getAsDouble();

                if( !jsonOpenSkyStateArr.get(10).isJsonNull())                    
                    openSkyRaw.headingDecDegFromNorth0 = jsonOpenSkyStateArr.get(10).getAsDouble();

                if( !jsonOpenSkyStateArr.get(11).isJsonNull())                    
                    openSkyRaw.verticalRateMetersPerSec = jsonOpenSkyStateArr.get(11).getAsDouble();

                //if we have no callsign then its not useful
                if( openSkyRaw.callsign!=null && !openSkyRaw.callsign.isEmpty()){
                    openSkyRawList.add(openSkyRaw);
                }
            }
        }             
        return openSkyRawList;
    }    
    
    
     public static void main(String[] args) throws Exception {

        //start test code        
        ArrayList<OpenSkyRawAirTraffic> osraList = OpenSky.readAirTraffic();   
        
        for( OpenSkyRawAirTraffic osra: osraList ){
            System.out.print(osra.toString()+"\n");
            
            if( osra.onGround ){
                System.out.print("...."+osra.callsign +" is GROUNDED!!\n");
            }
            
            if( osra.latitude!=null && osra.longitude!=null){
                System.out.print(".... GEOHASH:"+com.github.davidmoten.geo.GeoHash.encodeHash(osra.latitude, osra.longitude) +"\n");                
            }
            
            //test byte streaming
            byte[] asBytes = osra.getAsBytes();
            System.out.print("....as bytes L="+asBytes.length+"\n");            
            OpenSkyRawAirTraffic osraRehydraded = OpenSkyRawAirTraffic.getFromBytes(asBytes);
            System.out.print("....rehydrated:"+osraRehydraded.callsign+"\n");
            
        }
        
        System.out.print("TOTAL FLIGHTS: "+osraList.size()+"\n");
        
        
        /*                
        com.github.davidmoten.geo.Coverage coverage =
            com.github.davidmoten.geo.GeoHash.coverBoundingBox(-0.489,51.28,0.236,51.686);
        System.out.print(".... GEOHASH FROM BBOX:"+coverage.toString());
        */
        
        //end test code
     }
    
}
