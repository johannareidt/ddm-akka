package de.ddm.helper;

import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.ResultCollector;
import de.ddm.structures.InclusionDependency;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class MinerManager {


    private final HashMap<String, List<CSVTable>> tables = new HashMap<>();
    private final HashMap<String, List<CSVColumn>> columns = new HashMap<>();
    private final Queue<DependencyWorker.Task> tasks = new ArrayDeque<>();
    private final HashMap<Pair<String, String>, List<Pair<String, String>>> dependencies = new HashMap<>();
    private final HashMap<Integer, DependencyWorker.Task> currentlyDoing = new HashMap<>();

    private final List<InclusionDependency> res = new ArrayList<>();


    public ResultCollector.ResultMessage getResults(){
        ResultCollector.ResultMessage rm =  new ResultCollector.ResultMessage(this.res);
        this.res.clear();
        return rm;
    }

    private void handleResults(List<InclusionDependency> ids){

        //List<InclusionDependency> ids = new ArrayList<>(message.getResult().getInclusionDependencies());
        if(ids.isEmpty()){
            ids.add(null);
        }

        for(InclusionDependency id: ids){
            if(id!= null) {
                Pair<String, String> referenced = new ImmutablePair<>(id.getReferencedFile().toString(), id.getReferencedAttributes()[0]);
                Pair<String, String> dependent = new ImmutablePair<>(id.getDependentFile().toString(), id.getDependentAttributes()[0]);
                if(!dependencies.containsKey(referenced)){
                    dependencies.put(referenced, new ArrayList<>());
                }
                dependencies.get(referenced).add(dependent);
                if(dependencies.containsKey(dependent)){
                    dependencies.get(referenced).addAll(dependencies.get(dependent));
                }
            }
        }

        this.res.addAll(ids);

        //this.resultCollector.tell(new ResultCollector.ResultMessage(ids));
    }

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

    public void saveAndNext(DependencyMiner.CompletionMessage message){
        if(message.getResult() != null) {
            if(message.getResult().isHasResult()) {
                handleResults(message.getResult().getInclusionDependencies());
                
            }
			/*
			if(message.result.pairs != null){
				this.pairs.addAll(message.result.pairs);
			}

			 */
            if(message.getResult().getColumn() != null){
                handleColumn(message.getResult().getColumn());
            }
            if(message.getResult().getTable() != null){
                handleTable(message.getResult().getTable());
            }
        }
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
