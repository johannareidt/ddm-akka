package de.ddm.helper;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

@Getter
public class EmptyPair {

    String columnFile1;
    String columnFile2;
    String columnName1;
    String columnName2;

    public EmptyPair(String columnFile1, String columnFile2, String columnName1, String columnName2) {
        this.columnFile1 = columnFile1;
        this.columnFile2 = columnFile2;
        this.columnName1 = columnName1;
        this.columnName2 = columnName2;
    }

    public EmptyPair(CSVColumn column, CSVColumn oc) {
        this.columnName1 = column.getColumnName();
        this.columnFile1 = column.getFilePath();
        this.columnFile2= oc.getFilePath();
        this.columnName2= oc.getColumnName();
    }

    public AnalyzePair transform(CSVTable t1, CSVTable t2){
        if(t1 != null  && t2 != null) {
            return new AnalyzePair(t1.getColumn(columnName1), t2.getColumn(columnName2));
        }
        return null;
    }

    @Override
    public String toString() {
        return "EmptyPair{" +
                "columnFile1='" + columnFile1 + '\'' +
                ", columnFile2='" + columnFile2 + '\'' +
                ", columnName1='" + columnName1 + '\'' +
                ", columnName2='" + columnName2 + '\'' +
                '}';
    }
}
