package de.ddm.helper;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

@AllArgsConstructor
@Getter
public class EmptyPair {

    String columnFile1;
    String columnFile2;
    String columnName1;
    String columnName2;

    public AnalyzePair transform(CSVTable t1, CSVTable t2){
        if(t1 != null  && t2 != null) {
            return new AnalyzePair(t1.getColumn(columnName1), t2.getColumn(columnName2));
        }
        return null;
    }


}
