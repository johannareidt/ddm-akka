package de.ddm.helper;

import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class AnalyzePair implements Serializable {

    private final CSVColumn column1;
    private final CSVColumn column2;
    @Setter
    private Comparator<String> comparator;


    public AnalyzePair(CSVColumn column1, CSVColumn column2) {
        this.column1 = column1;
        this.column2 = column2;
    }


    public void setSorter(Comparator<String> sorter){
        this.column1.setSorter(sorter);
        this.column2.setSorter(sorter);
    }


    public void sort(){
        this.column1.sort();
        this.column2.sort();
    }

    public boolean firstIsSubSetToSecond(){
        return this.column2.containsAll(this.column1);
    }

    public boolean secondIsSubSetToFirst(){
        return this.column1.containsAll(this.column2);
    }

    public boolean oneIsSubsetToTheOther(){
        return firstIsSubSetToSecond() || secondIsSubSetToFirst();
    }
}
