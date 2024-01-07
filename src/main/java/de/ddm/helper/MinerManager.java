package de.ddm.helper;

import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.structures.InclusionDependency;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

public class MinerManager {

    private static final Log log = new SimpleLog("MinerManager");
    private final TaskLoader loader = new TaskLoader();


    private final HashMap<String, List<CSVTable>> tables = new HashMap<>();
    private final HashMap<String, List<CSVColumn>> columns = new HashMap<>();
    private final Queue<DependencyWorker.Task> tasks = new ArrayDeque<>();
    //private final HashMap<Pair<String, String>, List<Pair<String, String>>> dependencies = new HashMap<>();
    private final HashMap<Integer, DependencyWorker.Task> currentlyDoing = new HashMap<>();

    private final List<InclusionDependency> res = new ArrayList<>();
    private final HashMap<String, HashMap<String, List<CSVColumn>>> nextTables = new HashMap<>();


    // INCLUSION-DEPENDENCIES



    /*
    private void filterResults(){
        while (!this.last.isEmpty()) {
            this.getResultsLastAdded();
            this.res = InclusionDependencyFilter.filter(this.res);
        }
    }

     */

    /*
    public List<InclusionDependency> getAllResults() {
        filterResults();
        return this.res;
    }

     */



    /*
    public List<InclusionDependency> getResultsLastAdded(){
        //this.addMetaResultsFromLast();
        //this.last.clear();
        List<InclusionDependency> temp = new ArrayList<>(InclusionDependencyFilter.filter(this.last));
        this.res.addAll(temp);
        this.last.clear();
        return temp;
    }

     */

    public void handleResults(List<InclusionDependency> ids) {
        if (ids != null){
            if (!ids.isEmpty()) {
                this.addTask(new DependencyWorker.FilterInclusionDependencies(ids));
            }
        }
        //this.addTask(new DependencyWorker.FilterInclusionDependendies(this.res));
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
                    tasks.add(new DependencyWorker.AnalyzeTask(new EmptyPair(column, oc)));
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
            this.tasks.add(new DependencyWorker.MergeBatchColumn(path, cn));
        }
        //this.tables.rem ove(path);

        this.nextTables.put(path, columns);
    }


    public void handleFilteredResult(List<InclusionDependency> filteredInclusionDependencies) {
        this.res.addAll(filteredInclusionDependencies);
    }





    //TASK-HANDLING


    public void addTask(DependencyWorker.Task task){
        this.tasks.add(task);
    }

    public DependencyWorker.Task nextTask(){
        DependencyWorker.Task task = tasks.poll();
        if(task == null){
            //checkAllIdleWorkerWithResult();
            return new DependencyWorker.LastFilter();
        }
        task.load(loader);
        return task;
    }

    /*
    private void checkAllIdleWorkerWithResult(){
        if(this.res.size()>0){
            for(DependencyWorker.Task t:this.currentlyDoing.values()){
                if(!(t instanceof DependencyWorker.WaitTask)){
                    return;
                }
            }
            log.info("No one works, results are here: filter and start end!");
            this.addTask(new DependencyWorker.LastFilter());
        }
    }

     */


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





    private CSVColumn get(
            String path,
            String columnName
    ){
        if(this.columns == null){
            throw new RuntimeException();
        }
        if(this.columns.get(path) == null){
            log.error("keine spalten hinterlegt in path: "+path);
            log.error("paths mit spalten: "+this.columns.keySet());
            throw new RuntimeException();
        }
        return this.columns.getOrDefault(path, new ArrayList<>())
                .stream()
                .filter(column ->
                        columnName
                                .equals(
                                        column.getColumnName()))
                .findFirst()
                .get();
    }

    public void lastDone() {
        log.info("IS DONE: ");
        this.res.stream().peek(id->log.info("Results: "+id.toString()));
    }

    public class TaskLoader {
        public AnalyzePair getAnalyzerPair(EmptyPair emptyPair) {
            return new AnalyzePair(
                    MinerManager.this.get(emptyPair.columnFile1, emptyPair.columnName1),
                    MinerManager.this.get(emptyPair.columnFile2, emptyPair.columnName2)
                    );
        }

        public List<CSVColumn> getColumns(String path, String cn) {
            List<CSVColumn> temp =  MinerManager.this.nextTables.get(path).get(cn);
            //MinerManager.this.nextTables.get(path).rem ove(cn);
            return temp;
        }

        public List<InclusionDependency> getAllResults() {
            return MinerManager.this.res;
        }
    }
}
