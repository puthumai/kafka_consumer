package com.java.kafkaconsumer.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Vehicle {

    public Vehicle(){

    }

    private String vehicleNumber;
    private String registeredProvince;
    private String registeredCity;
}
