package de.ddm.helper;

import de.ddm.structures.InclusionDependency;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class InclusionDependencyFilter {

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

    public static List<InclusionDependency> filter(List<InclusionDependency> ids){
        ids = ids.stream().filter(InclusionDependencyFilter::allowedInclusionDependency).collect(Collectors.toList());
        List<InclusionDependency> temp = new ArrayList<>();
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
    }
}
