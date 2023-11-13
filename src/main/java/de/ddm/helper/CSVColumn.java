package de.ddm.helper;

import com.beust.jcommander.internal.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

public class CSVColumn implements Serializable {

    @Getter
    private final String filePath;

    @Getter
    private final String columnName;

    @Getter
    private final String[] entries;

    @Getter
    @Setter
    @Nullable
    private Comparator<String> sorter;

    public CSVColumn(String filePath, String columnName, String[] entries){
        this.filePath = filePath;
        this.columnName = columnName;
        this.entries = entries;
    }

    public void sort(){
        Arrays.sort(this.entries, sorter);
    }

    public boolean containsAll(CSVColumn column){
        return new HashSet<>(Arrays.asList(this.entries)).containsAll(Arrays.asList(column.entries));
    }




}
