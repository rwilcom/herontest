package com.nextcentury.herontest.dto;


/**
 *
 * 
 */
public class ActivityAlert extends Alert {

    public ActivityAlert(){}

    @Override
    public String getType() {
        return "ACTIVITY_ALERT";
    }    
    
}
