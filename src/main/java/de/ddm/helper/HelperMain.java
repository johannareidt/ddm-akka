package de.ddm.helper;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.InputReader;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.InclusionDependency;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class HelperMain {

    static final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();


    public static CSVReader getReader(File inputFile) throws IOException, CsvValidationException {
        CSVReader reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
        String[] header = InputConfigurationSingleton.get().getHeader(inputFile);

        if (InputConfigurationSingleton.get().isFileHasHeader())
            reader.readNext();
        return reader;
    }

    public static String[] getHeader(File inputFile) throws IOException, CsvValidationException {
        return InputConfigurationSingleton.get().getHeader(inputFile);
    }

    public static List<String[]> readBatch(CSVReader reader) throws CsvValidationException, IOException {
        List<String[]> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            String[] line = reader.readNext();
            if (line == null)
                break;
            batch.add(line);
        }

        return batch;
    }

    public static void startFile(File file) throws CsvValidationException, IOException {
        CSVReader reader = getReader(file);
        String[] header = getHeader(file);
        readBatch(reader);

    }


    public static List<CSVTable> readBatches(String filepath, CSVReader reader) throws CsvValidationException, IOException {
        //List<List<String[]>> res = new ArrayList<>();
        List<CSVTable> tables = new ArrayList<>();
        List<String[]> last = new ArrayList<>();
        last = readBatch(reader);
        while (last.size() != 0){
            //res.add(last);
            //Create Table Task
            tables.add(new CSVTable(filepath, last));
            tables.get(tables.size()-1).split();
            last = readBatch(reader);
        }
        return tables;
        //Read Batch message
        /*
        if (message.getBatch().size() != 0) {
            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));

            this.tasks.add(new DependencyWorker.CreateTableTask(
                    inputFiles[message.id].getPath(),
                    message.batch));
        }else{
            System.out.println("Batch zero");
        }

         */
    }

    public static List<CSVReader> getReaders(List<File> files) throws CsvValidationException, IOException {
        List<CSVReader> readers = new ArrayList<>();
        for (File f: files){
            System.out.println(Arrays.toString(getHeader(f)));
            readers.add(getReader(f));
        }
        return readers;
    }

    public static HashMap<String, List<CSVTable>> read(List<File> files) throws CsvValidationException, IOException {
        HashMap<String, List<CSVTable>> tables = new HashMap<>();
        for(File file: files){
            CSVReader reader = getReader(file);
            tables.put(file.getPath(), readBatches(file.getPath(), reader));
            //read ReadBatchMessage from input reader
        }
        return tables;
    }

    public static List<EmptyPair> makePairs( HashMap<String, List<CSVTable>> tables){
        //MakePairs
        List<EmptyPair> res = new ArrayList<>();
        for(List<CSVTable> csvTables1: tables.values()){
            for(List<CSVTable> csvTables2: tables.values()){
                EmptyTable t1 = csvTables1.get(0).toEmpty();
                EmptyTable t2 = csvTables2.get(0).toEmpty();

                List<EmptyPair> pairs = new ArrayList<>();
                for(String col1: t1.getHeader()){
                    for(String col2: t2.getHeader()){
                        pairs.add(new EmptyPair(t1.getFilepath(), t2.getFilepath(), col1, col2));
                    }
                }
                //result.setPairs(pairs);
                //return result;
                res.addAll(pairs);
            }
        }
        return res;
    }

    public static HashMap<EmptyPair, InclusionDependency> analyzeTask(List<EmptyPair> pairs, HashMap<String, List<CSVTable>> tables){
        HashMap<EmptyPair, InclusionDependency> res =new HashMap<>();
        for(EmptyPair p: pairs){
            for (CSVTable t1 : tables.get(p.getColumnFile1())){
                for (CSVTable t2 : tables.get(p.getColumnFile2())){
                    AnalyzePair analyzePair = p.transform(
                            t1,
                            t2
                    );

                    InclusionDependency id = analyzePair.firstIsSubSetToSecond();
                    if(id == null){
                        id = analyzePair.secondIsSubSetToFirst();
                    }
                    res.put(p, id);
                }
            }

        }
        return res;
    }



    public static void main(String[] args) {
        File folder = new File("C:\\Users\\johan\\Documents\\GitHub\\ddm-akka\\data\\TPCH");
        List<File> files  = List.of(Objects.requireNonNull(folder.listFiles()));
        HashMap<String, List<CSVTable>> tables = new HashMap<>();
        try {
            tables = read(files);
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }
        List<EmptyPair> pairs = makePairs(tables);
        HashMap<EmptyPair, InclusionDependency> results = analyzeTask(pairs, tables);



    }
}
