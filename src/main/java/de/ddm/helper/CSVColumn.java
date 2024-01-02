package de.ddm.helper;

import com.beust.jcommander.internal.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

public class CSVColumn implements Serializable {

    @Getter
    private final String filePath;

    @Getter
    private final String columnName;

    private final HashSet<String> entries;

    public CSVColumn(String filePath, String columnName, String[] entries){
        this.filePath = filePath;
        this.columnName = columnName;
        this.entries = new HashSet<>(Arrays.asList(entries));
    }

    /*
    public void sort(){
        Arrays.sort(this.entries, sorter);
    }

     */

    public boolean containsAll(CSVColumn column){
        return this.entries.containsAll(column.entries);
    }


    public Collection<String> getEntries() {
        return this.entries;
    }
}
