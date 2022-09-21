package com.example.kafkastreamdemo.entity;

public class Employee {

    private String name;
    private String position;
    private String company;
    private String exp;

    public Employee() {
    }

    public Employee(String name, String position, String company, String exp) {
        this.name = name;
        this.position = position;
        this.company = company;
        this.exp = exp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getExp() {
        return exp;
    }

    public void setExp(String exp) {
        this.exp = exp;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "name='" + name + '\'' +
                ", position='" + position + '\'' +
                ", company='" + company + '\'' +
                ", exp='" + exp + '\'' +
                '}';
    }
}
