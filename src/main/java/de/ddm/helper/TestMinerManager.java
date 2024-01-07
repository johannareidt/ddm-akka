package de.ddm.helper;

//import akka.stream.impl.fusing.Log;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.ResultCollector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestMinerManager {

    private static final Log log = new SimpleLog("TestMinerManager");

    public static void main(String[] args) {
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
        log.info("headers: "+headers);


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
                        files.get(id).getPath(),
                        batch,
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
                if (result.isHasResult()) {
                    minerManager.handleResults(result.getInclusionDependencies());
                    //this.resultCollector.tell(new ResultCollector.ResultMessage(minerManager.getResultsLastAdded()));
                }
                if (result.getColumn() != null) {
                    minerManager.handleColumn(result.getColumn());
                }
                if (result.getTable() != null) {
                    minerManager.handleTable(result.getTable());
                }
            }
            // I still don't know what task the worker could help me to solve ... but let me keep her busy.
            // Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
            //TODO:  start next Task

            //dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,  nextTask()));
            t = minerManager.nextTask();

        }



    }
}
