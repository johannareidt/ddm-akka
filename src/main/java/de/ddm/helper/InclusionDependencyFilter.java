package de.ddm.helper;

import de.ddm.structures.InclusionDependency;
import jnr.ffi.annotations.In;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class InclusionDependencyFilter {

    private static final Log log = new SimpleLog("InclusionDependencyFilter");

    private static List<InclusionDependency> metaInclusionDependencies(InclusionDependency id1, InclusionDependency id2){
        List<InclusionDependency> temp = new ArrayList<>();
        if(!allowedInclusionDependency(id1) || !allowedInclusionDependency(id2)){
            return temp;
        }

        if(id1.getDependentFile().getPath().equals(id2.getReferencedFile().getPath())){
            if(Objects.equals(id1.getDependentAttributes()[0], id2.getReferencedAttributes()[0])) {
                temp.add(new InclusionDependency(
                        id1.getReferencedFile(), id1.getReferencedAttributes(),
                        id2.getDependentFile(), id2.getDependentAttributes()));
            }
        }
        if(id1.getReferencedFile().getPath().equals(id2.getDependentFile().getPath())){
            if(Objects.equals(id1.getDependentAttributes()[0], id2.getReferencedAttributes()[0])) {
                temp.add(new InclusionDependency(
                        id2.getReferencedFile(), id2.getReferencedAttributes(),
                        id1.getDependentFile(), id1.getDependentAttributes()));
            }
        }
        return temp.stream().filter(InclusionDependencyFilter::allowedInclusionDependency).collect(Collectors.toList());
    }


    private static boolean allowedInclusionDependency(InclusionDependency id){
        if(id == null){
            return false;
        }
        return !id.getReferencedFile().getPath().equals(id.getDependentFile().getPath());
    }

    private static List<InclusionDependency> filterWithTemp(List<InclusionDependency> ids, List<InclusionDependency> toAdd){
        log.info("filterWithTemp: ");
        toAdd.removeIf(ids::contains);
        if(toAdd.isEmpty()){
            log.info("filterWithTemp: temp is empty: ");
            return new ArrayList<>(new HashSet<>(ids));
        }
        ids.addAll(toAdd);
        List<InclusionDependency> temp = new ArrayList<>();
        for(InclusionDependency id: ids){
            for(InclusionDependency j: toAdd) {
                temp.addAll(metaInclusionDependencies(id, j));
            }

        }
        return filterWithTemp(ids, temp);
    }

    public static List<InclusionDependency> filter(List<InclusionDependency> ids){
        log.info("filter:  ");
        ids = new ArrayList<>(new HashSet<>( ids.stream().filter(InclusionDependencyFilter::allowedInclusionDependency).collect(Collectors.toList())));
        List<InclusionDependency> temp = new ArrayList<>();
        for(InclusionDependency id: ids){
            for(InclusionDependency j: ids){
                temp.addAll(metaInclusionDependencies(id, j));
            }
        }
        return filterWithTemp(ids, temp);



        /*
        boolean added = true;
        int from = 0;
        while(added) {
            for (InclusionDependency id1 : ids.subList(from, ids.size())) {
                for (InclusionDependency id2 : ids) {
                    temp.addAll(metaInclusionDependencies(id1, id2));
                }
            }
            from = ids.size();
            ids.addAll(temp);
            added = !temp.isEmpty();
            temp.clear();
        }
        return ids;

         */
        //return ids;
    }


    public static List<InclusionDependency> getMoreWithTemp(List<InclusionDependency> ids, List<InclusionDependency> toAdd) {
        log.info("getMoreWithTemp:  ");
        toAdd.removeIf(ids::contains);
        if(toAdd.isEmpty()){
            log.info("getMoreWithTemp: temp is empty: ");
            return new ArrayList<>();
        }
        ids.addAll(toAdd);
        List<InclusionDependency> temp = new ArrayList<>();
        for(InclusionDependency id: ids){
            for(InclusionDependency j: toAdd) {
                temp.addAll(metaInclusionDependencies(id, j));
            }

        }
        toAdd.addAll(getMoreWithTemp(ids, new ArrayList<>(new HashSet<>(temp))));
        return toAdd;
    }
    public static List<InclusionDependency> getMore(List<InclusionDependency> ids) {
        log.info("getMore:  ");
        ids = new ArrayList<>(new HashSet<>( ids.stream().filter(InclusionDependencyFilter::allowedInclusionDependency).collect(Collectors.toList())));
        List<InclusionDependency> temp = new ArrayList<>();
        for(InclusionDependency id: ids){
            for(InclusionDependency j: ids){
                temp.addAll(metaInclusionDependencies(id, j));
            }
        }
        return new ArrayList<>(new HashSet<>(getMoreWithTemp(ids, temp)));
    }
}
