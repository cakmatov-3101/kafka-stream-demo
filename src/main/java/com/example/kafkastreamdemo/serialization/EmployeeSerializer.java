package com.example.kafkastreamdemo.serialization;

import com.example.kafkastreamdemo.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EmployeeSerializer implements Serializer<Employee> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }


    @Override
    public byte[] serialize(String s, Employee employee) {
        try {
            if (employee == null){
                System.out.println("Null received at serializing");
                return null;
            }
            System.out.println("Serializing...");
            return objectMapper.writeValueAsBytes(employee);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Coordinates to byte[]");
        }
    }

    @Override
    public void close() {
    }
}
