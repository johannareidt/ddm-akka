package de.ddm.helper;

//import akka.stream.impl.fusing.Log;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.ResultCollector;
import de.ddm.structures.InclusionDependency;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestMinerManager {

    private static final Log log = new SimpleLog("TestMinerManager");

    public static void main(String[] args) {
        List<InclusionDependency> res = new ArrayList<>();
        MinerManager minerManager = new MinerManager();
        List<File> files = TestHelper.getFiles();
        log.info("files: "+files);
        log.info("files len: "+files.size());
        //HelperMain.getReader();
        List<CSVReader> readers = new ArrayList<>();
        try {
            readers = TestHelper.getReaders(files);
            log.info("readers: read");;
            log.info("readers len: "+readers.size());
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }



        //Beginning ReadBatchMessages
        //          ReadHeaderMessages
        List<String[]> headers = TestHelper.getHeaders(files);
        log.info("headers: "+headers.stream().map(Arrays::toString).collect(Collectors.toList()));


        int id = 0;
        for(CSVReader r: readers){
            List<String[]> batch = new ArrayList<>();
            try {
                batch = TestHelper.readBatch(r);
                log.info("batch: read");
            } catch (CsvValidationException | IOException e) {
                throw new RuntimeException(e);
            }
            while(batch.size() !=0) {
                //if (message.getBatch().size() != 0) {
                //this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
                DependencyWorker.CreateTableTask ctt = new DependencyWorker.CreateTableTask(
                        batch,
                        files.get(id).getPath(),
                        headers.get(id));
                //Handle createTableTask answer answer
                minerManager.handleTable(ctt.handle().getTable());
                log.info("handleTable: (ctt: handle).table");


                try {
                    batch = TestHelper.readBatch(r);
                    log.info("batch: read");
                } catch (CsvValidationException | IOException e) {
                    throw new RuntimeException(e);
                }
                //else{


            }
            //Table finish
            minerManager.readTableFinish(files.get(id).getPath());
            log.info("readTableFinish: done");
            //}


            id = id + 1;
        }


        //Test doNext Task
        DependencyWorker.Task t= minerManager.nextTask();
        while ( !( t instanceof DependencyWorker.WaitTask)){
            DependencyMiner.Result result = t.handle();
            if (result != null) {
                if(result.isHasFilteredResult()){
                    log.info("nextTask: handleFilteredResult ");
                    minerManager.handleFilteredResult(result.getFilteredInclusionDependencies());
                    res.addAll(result.getFilteredInclusionDependencies());
                }
                if (result.isHasResult()) {
                    log.info("nextTask: handleResults ");
                    minerManager.handleResults(result.getInclusionDependencies());
                    //this.resultCollector.tell(new ResultCollector.ResultMessage(minerManager.getResultsLastAdded()));
                    //log.info("nextTask: get last added res: "+minerManager.getResultsLastAdded());
                }
                if (result.getColumn() != null) {
                    log.info("nextTask: handleColumn ");
                    minerManager.handleColumn(result.getColumn());
                }
                if (result.getTable() != null) {
                    log.info("nextTask: getTable ");
                    minerManager.handleTable(result.getTable());
                }
            }
            // I still don't know what task the worker could help me to solve ... but let me keep her busy.
            // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
            //TODO:  start next Task

            //dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,  nextTask()));
            t = minerManager.nextTask();

        }

        log.info("almost done: ");
        //minerManager.getResultsLastAdded();
        //log.info(minerManager.getAllResults());

        TestHelper.compareWithSolution(res);



    }
}
