package de.ddm.helper;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@NoArgsConstructor
public class Results implements Serializable {
    @Getter
    private final HashMap<AnalyzePair, Boolean> results = new HashMap<>();

    public void add(AnalyzePair analyzePair, Boolean res){
        this.results.put(analyzePair, res);
    }

    public Set<AnalyzePair> getAnalyzerPairs(){
        return this.results.keySet();
    }

    public List<AnalyzePair> getUnaryPairs(){
        List<AnalyzePair> res = new ArrayList<>();
        this.getResults().forEach((a,b) -> {if(b) res.add(a);});
        return res;
    }



}
