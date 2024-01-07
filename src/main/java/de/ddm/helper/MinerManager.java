package de.ddm.helper;

import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.structures.InclusionDependency;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

public class MinerManager {

    private static final Log log = new SimpleLog("MinerManager");


    private final HashMap<String, List<CSVTable>> tables = new HashMap<>();
    private final HashMap<String, List<CSVColumn>> columns = new HashMap<>();
    private final Queue<DependencyWorker.Task> tasks = new ArrayDeque<>();
    //private final HashMap<Pair<String, String>, List<Pair<String, String>>> dependencies = new HashMap<>();
    private final HashMap<Integer, DependencyWorker.Task> currentlyDoing = new HashMap<>();

    private final List<InclusionDependency> res = new ArrayList<>();
    private final List<InclusionDependency> last = new ArrayList<>();



    // INCLUSION-DEPENDENCIES

    private void addMetaResultsFromLast(){
        List<InclusionDependency> temp = new ArrayList<>(last);
        //last.clear();
        for(InclusionDependency id: res){
            for(InclusionDependency toAdd: temp){
                last.addAll(metaInclusionDependencies(id, toAdd));
            }
        }
        res.addAll(temp);
    }


    private List<InclusionDependency> metaInclusionDependencies(InclusionDependency id1, InclusionDependency id2){
        List<InclusionDependency> temp = new ArrayList<>();
        if(id1.getDependentFile().getPath().equals(id2.getReferencedFile().getPath())){
            temp.add(new InclusionDependency(
                    id1.getReferencedFile(), id1.getReferencedAttributes(),
                    id2.getDependentFile(), id2.getDependentAttributes()));
        }
        if(id1.getReferencedFile().getPath().equals(id2.getDependentFile().getPath())){
            temp.add(new InclusionDependency(
                    id2.getReferencedFile(), id2.getReferencedAttributes(),
                    id1.getDependentFile(), id1.getDependentAttributes()));
        }
        return temp;
    }

    private List<InclusionDependency> checkAllInclusionDependency(List<InclusionDependency> ids){
        while (ids.contains(null)){
            ids.remove(null);
        }
        List<InclusionDependency> temp = new ArrayList<>();
        for(InclusionDependency checkAgainst: res){
            ids.forEach(i-> temp.addAll(metaInclusionDependencies(checkAgainst, i)));
        }
        ids.addAll(temp);
        temp.clear();
        for(InclusionDependency checkAgainst: ids){
            ids.forEach(i-> temp.addAll(metaInclusionDependencies(checkAgainst, i)));
        }
        ids.addAll(temp);
        return ids;
    }




    public List<InclusionDependency> getResultsLastAdded(){
        this.addMetaResultsFromLast();
        //this.last.clear();
        List<InclusionDependency> temp = new ArrayList<>(this.last);
        this.res.addAll(this.last);
        this.last.clear();
        return temp;
    }

    public void handleResults(List<InclusionDependency> ids){
        this.last.addAll(this.checkAllInclusionDependency(ids));
    }




    //READING

    public void handleColumn(CSVColumn column){
        //CSVColumn column = message.getResult().getColumn();

        if(!this.columns.containsKey(column.getFilePath())) {
            this.columns.put(column.getFilePath(), new ArrayList<>());
        }


        this.columns.get(column.getFilePath()).add(column);


        for(String path: this.columns.keySet()){
            if(!Objects.equals(path, column.getFilePath())){
                for (CSVColumn oc : this.columns.get(path)){
                    tasks.add(new DependencyWorker.AnalyzeTask(new AnalyzePair(column, oc)));
                }
            }

        }
    }

    public void handleTable(CSVTable table){

        //CSVTable res = message.getResult().getTable();

        if(this.tables.containsKey(table.getFilepath())){
            this.tables.get(table.getFilepath()).add(table);

					/*
					for(EmptyPair p: this.pairs){
						if(Objects.equals(p.getColumnFile1(), res.getFilepath())){
							for (CSVTable t2 : this.tables.get(p.getColumnFile2())) {
								tasks.add(new DependencyWorker.AnalyzeTask(p.transform(
										res,
										t2
								)));
							}
						}if(Objects.equals(p.getColumnFile2(), res.getFilepath())){
							for (CSVTable t1 : this.tables.get(p.getColumnFile1())) {
								tasks.add(new DependencyWorker.AnalyzeTask(p.transform(
										t1,
										res
								)));
							}
						}
					}

					 */

        }else {
            List<CSVTable> ts = new ArrayList<>();
            ts.add(table);
            this.tables.put(table.getFilepath(), ts);



            //Make pairs after last empty batch
					/*
					for(List<CSVTable> l: this.tables.values()) {
						tasks.add(new DependencyWorker.MakePairsTask(res.toEmpty(), l.get(0).toEmpty()));
					}

					 */
        }
        //this.tables.put(res.getFilepath(), res);
    }

    public void readTableFinish(String path){
        //System.out.println("Batch zero");
        HashMap<String, List<CSVColumn>> columns = new HashMap<>();
        if(this.tables.containsKey(path)) {
            for (CSVTable t : this.tables.get(path)) {
                for (String cn : t.getColumnNames()) {
                    if (!columns.containsKey(cn)) {
                        columns.put(cn, new ArrayList<>());
                    }
                    columns.get(cn).add(t.getColumn(cn));
                }
            }
        }
        for (String cn: columns.keySet()) {
            this.tasks.add(new DependencyWorker.MergeBatchColumn(path, cn, columns.get(cn)));
        }
    }





    //TASK-HANDLING


    public void addTask(DependencyWorker.Task task){
        this.tasks.add(task);
    }

    public DependencyWorker.Task nextTask(){
        DependencyWorker.Task task = tasks.poll();
        if(task == null){
            return new DependencyWorker.WaitTask();
        }
        return task;
    }


    public void putCurrently(int indexOfWorker, DependencyWorker.Task task) {
        this.currentlyDoing.put(indexOfWorker, task);
    }

    public boolean isCurrentlyWorking(int indexOfWorker){
        return this.currentlyDoing.containsKey(indexOfWorker);
    }

    public int workingSize(){
        return this.currentlyDoing.size();
    }

    public void workerStopped(int workerID){
        this.tasks.add(this.currentlyDoing.get(workerID));
        this.currentlyDoing.remove(workerID);
    }

}
