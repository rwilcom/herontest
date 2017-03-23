package com.nextcentury.opensky;

import java.io.IOException;
import java.util.Date;

/**
 *
 * 
 */
public class OpenSkyRawAirTraffic extends ByteSerializable {

    public OpenSkyRawAirTraffic(){}

    public Date lastReadDateTime;

    public String icao24TransponderAddr;
    public String callsign;
    public String originCountry;
    public Date timeOfPositionDateTime;
    public Date timeOfVelocityDateTime;
    public Double longitude;
    public Double latitude;
    public Double altitudeMeters;
    public Boolean onGround;
    public Double velocityMetersPerSec;
    public Double headingDecDegFromNorth0;
    public Double verticalRateMetersPerSec;

    @Override
    public String toString(){
        return "icao24TransponderAddr:"+icao24TransponderAddr+
               ";callsign:"+callsign+
               ";originCountry:"+originCountry+
               ";timeOfPositionDateTime:"+timeOfPositionDateTime+
               ";timeOfVelocityDateTime:"+timeOfVelocityDateTime+
               ";longitude:"+longitude+
               ";latitude:"+latitude+
               ";altitudeMeters:"+altitudeMeters+
               ";onGround:"+onGround+
               ";velocityMetersPerSec:"+velocityMetersPerSec+
               ";headingDecDegFromNorth0:"+headingDecDegFromNorth0+
               ";veticalRateMetersPerSec:"+verticalRateMetersPerSec;
    }   
    
    static public OpenSkyRawAirTraffic getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        return (OpenSkyRawAirTraffic)ByteSerializable.getFromBytes(objBytes);
    }
}
