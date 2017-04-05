package com.nextcentury.herontest.dto;


/**
 *
 * 
 */
public class LocationAlert extends Alert {

    public LocationAlert(){}

    @Override
    public String getType() {
        return "LOCATION_ALERT";
    }
}
