package de.ddm.helper;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Manager implements Serializable {

    /*
    @Getter
    private List<AnalyzePair> pairs;

    @Getter
    @Setter
    private List<String> filePaths;

    @Getter
    private List<CSVTable> tables;

    public void makePairs(){
        this.pairs = new ArrayList<>();
        for(int i = 0; i< this.tables.size()-1; i++){
            CSVTable tableI = this.tables.get(i);
            for(int j= i+1; j<this.tables.size(); j++){
                CSVTable tableJ = this.tables.get(j);
                for(String columnNameI: tableI.getColumnNames()) {
                    for(String columnNameJ: tableJ.getColumnNames()) {
                        pairs.add(new AnalyzePair(tableI.getColumn(columnNameI),
                                tableJ.getColumn(columnNameJ)));
                    }
                }
            }
        }
    }

    public List<AnalyzePair> getMissingAnalysePairs(List<AnalyzePair> done){
        ArrayList<AnalyzePair> missing =  new ArrayList<>(this.pairs);
        missing.removeAll(done);
        return missing;
    }

     */

}
