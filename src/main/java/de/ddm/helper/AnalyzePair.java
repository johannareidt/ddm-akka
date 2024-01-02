package de.ddm.helper;

import de.ddm.structures.InclusionDependency;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

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

    public InclusionDependency firstIsSubSetToSecond(){
        if(this.column2.containsAll(this.column1)){
            return new InclusionDependency(new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()},
                    new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()});
        }
        return null;
    }

    public InclusionDependency secondIsSubSetToFirst(){
        if(this.column2.containsAll(this.column1)){
            return new InclusionDependency(new File(this.column2.getFilePath()), new String[]{this.column2.getColumnName()},
                    new File(this.column1.getFilePath()), new String[]{this.column1.getColumnName()});
        }
        return null;
    }

    public EmptyPair toEmpty(){
        return new EmptyPair(column1.getFilePath(), column2.getFilePath(), column1.getColumnName(), column2.getColumnName());
    }

}
