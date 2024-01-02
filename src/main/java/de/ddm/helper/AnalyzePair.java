package de.ddm.helper;

import de.ddm.structures.InclusionDependency;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

public class AnalyzePair implements Serializable {

    @Getter
    private final CSVColumn column1;
    @Getter
    private final CSVColumn column2;
    @Setter
    private Comparator<String> comparator;


    public AnalyzePair(CSVColumn column1, CSVColumn column2) {
        this.column1 = column1;
        this.column2 = column2;
    }


    public  InclusionDependency getInclusionDependency(){
        boolean c1SubToc2 = false;
        boolean c2SubToc1 = false;

        int firstInSecond = firstInSecond();
        int firstNotInSecond = firstNotInSecond();
        int secondInFirst = secondInFirst();
        int secondNotInFirst = secondNotInFirst();

        if(firstInSecond>0 && firstNotInSecond<=firstInSecond*0.1){
            c1SubToc2 = true;
        }
        if(secondInFirst>0 && secondNotInFirst<=secondInFirst*0.1){
            c2SubToc1 = true;
        }

        if(c1SubToc2) {
            //colum1 subset to column2
            return new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                    new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()});
        }
        if(c2SubToc1) {
            //colum2 subset to column1
            return new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                    new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()});
        }
        return null;
    }


    public InclusionDependency firstIsSubSetToSecond(){
        if(this.column2.containsAll(this.column1)){
            return new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                    new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()});
        }
        return null;
    }

    public InclusionDependency secondIsSubSetToFirst(){
        //biglist.containsAll(littleList)
        if(this.column1.containsAll(this.column2)){
            return new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                    new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()});
        }
        return null;
    }

    public int firstInSecond(){
        ArrayList<String> res = new ArrayList<>();
        this.column1.getEntries().iterator().forEachRemaining(e->
        {
            if(column2.getEntries().contains(e)){
                res.add(e);
        }});
        return new HashSet<String>(res).size();
    }

    public int firstNotInSecond(){
        ArrayList<String> res = new ArrayList<>();
        this.column1.getEntries().iterator().forEachRemaining(e->
        {
            if(!column2.getEntries().contains(e)){
                res.add(e);
            }});
        return new HashSet<String>(res).size();
    }
    public int secondInFirst(){
        ArrayList<String> res = new ArrayList<>();
        this.column2.getEntries().iterator().forEachRemaining(e->
        {
            if(column1.getEntries().contains(e)){
                res.add(e);
            }});
        return new HashSet<String>(res).size();
    }

    public int secondNotInFirst(){
        ArrayList<String> res = new ArrayList<>();
        this.column2.getEntries().iterator().forEachRemaining(e->
        {
            if(!column1.getEntries().contains(e)){
                res.add(e);
            }});
        return new HashSet<String>(res).size();
    }


    public EmptyPair toEmpty(){
        return new EmptyPair(column1.getFilePath(), column2.getFilePath(), column1.getColumnName(), column2.getColumnName());
    }

}
