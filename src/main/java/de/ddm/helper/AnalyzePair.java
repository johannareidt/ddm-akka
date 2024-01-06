package de.ddm.helper;

import de.ddm.structures.InclusionDependency;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class AnalyzePair implements Serializable {

    @Getter
    private final CSVColumn column1;
    @Getter
    private final CSVColumn column2;


    public AnalyzePair(CSVColumn column1, CSVColumn column2) {
        this.column1 = column1;
        this.column2 = column2;
    }


    void logged(String pre){

        boolean c1SubToc2 = false;
        boolean c2SubToc1 = false;

        int min = Math.min(this.column1.getEntries().size(), this.column2.getEntries().size());

        int firstInSecond = firstInSecond();
        int firstNotInSecond = firstNotInSecond();

        int secondInFirst = secondInFirst();
        int secondNotInFirst = secondNotInFirst();
        System.out.println(pre+ " Pair: firstInSecond: "+firstInSecond);
        System.out.println(pre+ " Pair: firstInSecond>1: "+(firstInSecond>0));
        System.out.println(pre+ " Pair: firstInSecond>=firstNotInSecond: "+(firstInSecond>=firstNotInSecond));
        System.out.println(pre+ " Pair: firstInSecond*0.1: "+firstInSecond*0.1);
        System.out.println(pre+ " Pair: firstNotInSecond: "+firstNotInSecond);
        System.out.println(pre+ " Pair: firstNotInSecond<=firstInSecond*0.1: "+(firstNotInSecond<=firstInSecond*0.1));
        System.out.println(pre+ " Pair: firstNotInSecond<=1: "+(firstNotInSecond<=1));
        System.out.println(pre+ " Pair: firstInSecond*0.1<=1: "+(firstInSecond*0.1<=1));
        System.out.println(pre+ " Pair: secondInFirst: "+secondInFirst);
        System.out.println(pre+ " Pair: secondInFirst>1: "+(secondInFirst>0));
        System.out.println(pre+ " Pair: secondInFirst>=secondNotInFirst: "+(secondInFirst>=secondNotInFirst));
        System.out.println(pre+ " Pair: secondInFirst*0.1: "+(secondInFirst*0.1));
        System.out.println(pre+ " Pair: secondNotInFirst: "+secondNotInFirst);
        System.out.println(pre+ " Pair: secondNotInFirst<=secondInFirst*0.1: "+(secondNotInFirst<=secondInFirst*0.1));
        System.out.println(pre+ " Pair: secondNotInFirst<=1: "+(secondNotInFirst<=1));
        System.out.println(pre+ " Pair: secondInFirst*0.1<=1: "+(secondInFirst*0.1<=1));

    }

    public List<InclusionDependency> getInclusionDependency(){
        List<InclusionDependency> id = new ArrayList<>();
        //boolean c1SubToc2 = false;
        //boolean c2SubToc1 = false;

        //int min = Math.min(this.column1.getEntries().size(), this.column2.getEntries().size());

        int firstInSecond = firstInSecond();
        int firstNotInSecond = firstNotInSecond();

        int secondInFirst = secondInFirst();
        int secondNotInFirst = secondNotInFirst();

        System.out.println();
        System.out.println("Pair: "+this.toEmpty().toString());

        if(firstInSecond>0){
            if(firstInSecond==1 && firstNotInSecond==0){
                System.out.println("Pair: "+"firstInSecond==1 && firstNotInSecond==0");
                id.add( new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                        new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()}));
                System.out.println(id.get(0).toString());
            }

            if(firstInSecond>=firstNotInSecond) {
                if (firstNotInSecond<=firstInSecond*0.1) {

                    //c1SubToc2 = true;

                    System.out.println("Pair: "+"firstInSecond>1, firstInSecond>=firstNotInSecond, firstNotInSecond<=firstInSecond*0.1");


                    id.add( new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                            new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()}));
                    System.out.println(id.get(0).toString());

                }
                else if (firstNotInSecond <= 1) {
                    if(firstInSecond*0.1<=1) {
                        System.out.println("Pair: " + "firstInSecond>1, firstInSecond>=firstNotInSecond, firstNotInSecond <= 1, firstInSecond*0.1<=1");

                        //c1SubToc2 = true;

                        id.add(new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                                new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()}));

                        System.out.println(id.get(0).toString());
                    }

                }
            }
        }
        if(secondInFirst>0){
            if(secondInFirst==1 && secondNotInFirst==0){
                System.out.println("Pair: "+"secondInFirst==1 && secondNotInFirst==0");
                id.add( new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                        new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()}));
                System.out.println(id.get(0).toString());
            }
            if(secondInFirst>=secondNotInFirst) {
                if (secondNotInFirst <= secondInFirst * 0.1) {

                    //c2SubToc1 = true;


                    System.out.println("Pair: "+"secondInFirst>1, secondInFirst>=secondNotInFirst, secondNotInFirst <= secondInFirst * 0.1");

                    id.add( new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                            new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()}));

                    System.out.println(id.get(id.size()-1).toString());

                }
                else if (secondNotInFirst <= 1) {

                    if(secondInFirst*0.1<=1) {
                        //c2SubToc1 = true;
                        System.out.println("Pair: " + "secondInFirst>1, secondInFirst>=secondNotInFirst, secondInFirst <= 1, secondInFirst*0.1<=1");
                        id.add(new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                                new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()}));

                        System.out.println(id.get(id.size()-1).toString());
                    }

                }
            }
        }
        /*

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

         */
        if(id.isEmpty()){
            id.add(null);
        }
        return id;
    }



    InclusionDependency firstIsSubSetToSecond(){
        if(this.column2.containsAll(this.column1)){
            return new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                    new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()});
        }
        return null;
    }

    InclusionDependency secondIsSubSetToFirst(){
        //biglist.containsAll(littleList)
        if(this.column1.containsAll(this.column2)){
            return new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                    new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()});
        }
        return null;
    }

    int firstInSecond(){
        ArrayList<String> res = new ArrayList<>();
        this.column1.getEntries().iterator().forEachRemaining(e->
        {
            if(column2.getEntries().contains(e)){
                res.add(e);
        }});
        return new HashSet<String>(res).size();
    }

    int firstNotInSecond(){
        ArrayList<String> res = new ArrayList<>();
        this.column1.getEntries().iterator().forEachRemaining(e->
        {
            if(!column2.getEntries().contains(e)){
                res.add(e);
            }});
        return new HashSet<String>(res).size();
    }
    int secondInFirst(){
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
