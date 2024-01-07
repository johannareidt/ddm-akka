package de.ddm.helper;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.structures.InclusionDependency;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class HelperMain {

    static final HashMap<String, List<CSVColumn>> columns = new HashMap<>();


    public static void startFile(File file) throws CsvValidationException, IOException {
        CSVReader reader = TestHelper.getReader(file);
        String[] header = TestHelper.getHeader(file);
        TestHelper.readBatch(reader);

    }



    public static List<EmptyPair> makePairs( HashMap<String, List<CSVTable>> tables){
        //MakePairs
        List<EmptyPair> res = new ArrayList<>();
        for(List<CSVTable> csvTables1: tables.values()){
            for(List<CSVTable> csvTables2: tables.values()){
                EmptyTable t1 = csvTables1.get(0).toEmpty();
                EmptyTable t2 = csvTables2.get(0).toEmpty();

                if(!Objects.equals(t1.getFilepath(), t2.getFilepath())) {

                    List<EmptyPair> pairs = new ArrayList<>();
                    for (String col1 : t1.getHeader()) {
                        for (String col2 : t2.getHeader()) {
                            pairs.add(new EmptyPair(t1.getFilepath(), t2.getFilepath(), col1, col2));
                        }
                    }
                    //result.setPairs(pairs);
                    //return result;
                    res.addAll(pairs);
                }
            }
        }
        return res;
    }

    public static void analyzeTask(InclusionDependency id, String pre){
        AnalyzePair analyzePair = new AnalyzePair(
                columns.get(id.getDependentFile().getPath())
                        .stream().filter(column ->
                                Objects.equals(column.getColumnName(),
                                        id.getDependentAttributes()[0]))
                        .collect(Collectors.toList())
                        .get(0),
                columns.get(id.getReferencedFile().getPath())
                        .stream().filter(column ->
                                Objects.equals(column.getColumnName(),
                                        id.getReferencedAttributes()[0]))
                        .collect(Collectors.toList())
                        .get(0));
        analyzePair.logged(pre);
        /*
        System.out.println(pre+" matched: firstIsSubSetToSecond "+analyzePair.firstIsSubSetToSecond());
        System.out.println(pre+" matched: firstInSecond "+analyzePair.firstInSecond());
        System.out.println(pre+" matched: firstNotInSecond "+analyzePair.firstNotInSecond());


        System.out.println(pre+" otherway: firstIsSubSetToSecond "+analyzePair.firstIsSubSetToSecond());
        System.out.println(pre+" otherway: firstInSecond "+analyzePair.firstInSecond());
        System.out.println(pre+" otherway: firstNotInSecond "+analyzePair.firstNotInSecond());

         */
        /*
        System.out.println(pre+" secondIsSubSetToFirst "+analyzePair.secondIsSubSetToFirst());
        System.out.println(pre+" secondInFirst "+analyzePair.secondInFirst());
        System.out.println(pre+" secondNotInFirst "+analyzePair.secondNotInFirst());

         */
    }

    public static void analyzeTask(HashMap<String, List<CSVTable>> tables, InclusionDependency id, String pre, List<String> solutions){
        TestHelper.loadColumns(tables);
        AnalyzePair analyzePair = new AnalyzePair(
                columns.get(id.getDependentFile().getPath())
                        .stream().filter(column ->
                                Objects.equals(column.getColumnName(),
                                        id.getDependentAttributes()[0]))
                        .collect(Collectors.toList())
                        .get(0),
                columns.get(id.getReferencedFile().getPath())
                        .stream().filter(column ->
                                Objects.equals(column.getColumnName(),
                                        id.getReferencedAttributes()[0]))
                        .collect(Collectors.toList())
                        .get(0));
        System.out.println("\n");
        System.out.println(id.toString());
        List<InclusionDependency> temp = analyzePair.getInclusionDependency();
        for(InclusionDependency t: temp){
            if(t != null) {
                System.out.println(pre+t.toString() + " with in solutions: " + solutions.contains(t.toString()));
            }else{
                System.out.println(pre+"error: "+id.toString());
            }
        }
        analyzePair.logged(pre);

    }

    public static HashMap<EmptyPair, List<InclusionDependency>> analyzeTask(HashMap<String, List<CSVTable>> tables){
        HashMap<EmptyPair, List<InclusionDependency>>res =new HashMap<>();

        HashMap<CSVColumn, List<CSVColumn>> temp = new HashMap<>();

        TestHelper.loadColumns(tables);

        for(String path1: columns.keySet()){
            for(String path2: columns.keySet()){
                if(!Objects.equals(path1, path2)) {
                    for(CSVColumn c1: columns.get(path1)) {
                        for(CSVColumn c2: columns.get(path2)) {
                            AnalyzePair analyzePair = new AnalyzePair(c1, c2);
                            res.put(analyzePair.toEmpty(), analyzePair.getInclusionDependency());
                            //temp.put(analyzePair.getColumn1(),
                            for(InclusionDependency i: res.get(analyzePair.toEmpty())) {
                                if(Objects.equals(i.getReferencedAttributes()[0], analyzePair.getColumn1().getColumnName())) {
                                    if (!temp.containsKey(analyzePair.getColumn1())) {
                                        temp.put(analyzePair.getColumn1(), new ArrayList<>());
                                    }
                                    temp.get(analyzePair.getColumn1()).add(analyzePair.getColumn2());
                                }
                                if(Objects.equals(i.getReferencedAttributes()[0], analyzePair.getColumn2().getColumnName())) {

                                    if (!temp.containsKey(analyzePair.getColumn2())) {
                                        temp.put(analyzePair.getColumn2(), new ArrayList<>());
                                    }
                                    temp.get(analyzePair.getColumn2()).add(analyzePair.getColumn1());
                                }
                            }
                        }
                    }
                }
            }
        }
        return res;
    }


    public static void analyzeAll(){

        List<String> solution = new ArrayList<>();
        try {
            solution = TestHelper.getResults();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File folder = new File("C:\\Users\\johan\\Documents\\GitHub\\ddm-akka\\data\\TPCH");
        List<File> files  = List.of(Objects.requireNonNull(folder.listFiles()));
        //System.out.println(files);
        HashMap<String, List<CSVTable>> tables = new HashMap<>();
        try {
            tables = TestHelper.read(files);
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }
        List<EmptyPair> pairs = makePairs(tables);

        //System.out.println(pairs);
        HashMap<EmptyPair, List<InclusionDependency>> results = analyzeTask( tables);
        //System.out.println(results);

        /*
        for(EmptyPair p: results.keySet()){
            List<InclusionDependency> l = results.get(p);
            if(l.isEmpty()){
                System.out.println("EMPTY: "+ p);
            }

        }

         */


        /*
        for(EmptyPair p: results.keySet()){
            List<InclusionDependency> l = results.get(p);
            if(!l.isEmpty()){
                if(l.contains(null)){
                    System.out.println("Also EMPTY: "+ p.columnName1+"->"+p.columnName2+":"+p.columnName1+"->"+p.columnName2);
                    for(InclusionDependency id : l) {
                        if(id != null) {
                            if (solution.contains(id.toString())) {
                                System.out.println("Is in results: " + solution.contains(id.toString()));
                                System.out.println("List: "+l);
                            }
                        }else{
                            System.out.println("null");
                        }
                    }
                }
            }

        }

         */
        /*
        for(EmptyPair p: results.keySet()){
            List<InclusionDependency> l = results.get(p);
            if(!l.isEmpty()){
                if(!l.contains(null)){
                    System.out.println("DEFINITELY: "+ l.get(0));
                    System.out.println("Is in results: "+solution.contains(l.get(0).toString()));
                }
            }
        }

         */

        //HashMap<InclusionDependency, Map<String, Long>> countingMap = new HashMap<>();

        List<InclusionDependency> match = new ArrayList<>();
        List<InclusionDependency> falseMatch = new ArrayList<>();
        List<InclusionDependency> shouldBeMatch = new ArrayList<>();
        List<InclusionDependency> correctNotMatch = new ArrayList<>();

        for(EmptyPair p: results.keySet()){
            List<InclusionDependency> l = results.get(p);
            if(!l.isEmpty()){

                InclusionDependency temp1 = new InclusionDependency(new File(p.getColumnFile1()), new String[]{p.getColumnName1()},
                        new File(p.getColumnFile2()), new String[]{p.getColumnName2()});
                InclusionDependency temp2 = new InclusionDependency(new File(p.getColumnFile2()), new String[]{p.getColumnName2()},
                        new File(p.getColumnFile1()), new String[]{p.getColumnName1()});


                /*
                int nulloccurrences = Collections.frequency(l, null);
                int temp1occurrences = Collections.frequency(l, temp1);
                int temp2occurrences = Collections.frequency(l, temp2);

                 */


                /*
                Map<String, Long> counts =
                        results.get(p).stream().collect(Collectors.groupingBy(e->{if(e == null){
                            return "null";
                        } return e.toString();}, Collectors.counting()));

                countingMap.put(temp1, counts);
                countingMap.put(temp2, counts);

                 */



                if(l.contains(null)
                ){
                    if(solution.contains(temp1.toString())){
                        shouldBeMatch.add(temp1);
                    }else {
                        correctNotMatch.add(temp1);
                    }
                    if(solution.contains(temp2.toString())){
                        shouldBeMatch.add(temp2);
                    }else {
                        correctNotMatch.add(temp2);
                    }
                }else if(l.contains(temp1)){
                    if(solution.contains(temp1.toString())){
                        match.add(temp1);
                    }else {
                        falseMatch.add(temp1);
                    }
                }else if(l.contains(temp2)){
                    if(solution.contains(temp2.toString())){
                        match.add(temp2);
                    }else {
                        falseMatch.add(temp1);
                    }
                }else  {
                    throw new RuntimeException();
                }

                /*
                    if(counts.get(temp1.toString()) >= counts.get("null")){
                    if(solution.contains(temp1.toString())){
                        match.add(temp1);
                    }else {
                        falseMatch.add(temp1);
                    }
                }else if(counts.get(temp2.toString()) >= counts.get("null")){
                    if(solution.contains(temp2.toString())){
                        match.add(temp2);
                    }else {
                        falseMatch.add(temp1);
                    }
                }else{
                    if()
                }

                 */

                /*
                if (!counts.containsKey("null")) {
                    if(solution.contains(temp1.toString())){
                        match.add(p);
                    }
                    /*
                    if(temp1occurrences >= l.size()/2 -1){
                        if(solution.contains(temp1.toString())){
                            match.add(p);
                        }
                    }
                    if(temp2occurrences >= l.size()/2 -1){
                        if(solution.contains(temp2.toString())){
                            match.add(p);
                        }
                    }


                 */

                /*

                    if(solution.contains(temp2.toString())){
                        match.add(p);
                    }else {
                        falseMatch.add(p);
                    }
                    //System.out.println("DEFINITELY: "+ l.get(0));
                    //System.out.println("Is in results: "+solution.contains(l.get(0).toString()));
                } else{


                }

                 */
            }
        }

        System.out.println("match: length: "+match.size());
        for(InclusionDependency p: match) {
            //if(counts.get()) {
            System.out.println("match: " + p);
            analyzeTask( p, "match: ");
            //System.out.println("match: List: " +  countingMap.get(p));
            //}
        }

        System.out.println("correctNotMatch: length: "+correctNotMatch.size());
        /*
        for(InclusionDependency p: correctNotMatch) {
            System.out.println("correctNotMatch: " + p);
            //if(countingMap.get(p).keySet().size()!=1) {
            //    System.out.println("correctNotMatch: " + p);
            //    System.out.println("correctNotMatch: List: " + countingMap.get(p));
            //}
        }

         */

        System.out.println("falseMatch: length: "+falseMatch.size());
        for(InclusionDependency p: falseMatch) {
            System.out.println("falseMatch: " + p);
            analyzeTask( p, "falseMatch: ");

            /*
            System.out.println(columns.get(p.getDependentFile().getPath())
                    .stream().filter(column ->
                            Objects.equals(column.getColumnName(),
                                    p.getDependentAttributes()[0])).collect(Collectors.toList()).get(0).getEntries());
            System.out.println(columns.get(p.getReferencedFile().getPath())
                    .stream().filter(column ->
                            Objects.equals(column.getColumnName(),
                                    p.getReferencedAttributes()[0])).collect(Collectors.toList()).get(0).getEntries());


             */


            //System.out.println("falseMatch: List: " +  countingMap.get(p));
        }
        System.out.println("shouldBeMatch: length: "+shouldBeMatch.size());
        for(InclusionDependency p: shouldBeMatch) {
            System.out.println("shouldBeMatch: " + p);
            analyzeTask(p, "shouldBeMatch: ");
            /*
            System.out.println(columns.get(p.getDependentFile().getPath())
                    .stream().filter(column ->
                            Objects.equals(column.getColumnName(),
                                    p.getDependentAttributes()[0])).collect(Collectors.toList()).get(0).getEntries());
            System.out.println(columns.get(p.getReferencedFile().getPath())
                    .stream().filter(column ->
                            Objects.equals(column.getColumnName(),
                                    p.getReferencedAttributes()[0])).collect(Collectors.toList()).get(0).getEntries());

             */
            //System.out.println("shouldBeMatch: List: " +  countingMap.get(p));
        }




    }


    public static void analyzeSolutions(){
        List<String> solution = new ArrayList<>();
        try {
            solution = TestHelper.getResults();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File folder = new File("C:\\Users\\johan\\Documents\\GitHub\\ddm-akka\\data\\TPCH");
        List<File> files  = List.of(Objects.requireNonNull(folder.listFiles()));
        //System.out.println(files);
        HashMap<String, List<CSVTable>> tables = new HashMap<>();
        try {
            tables = TestHelper.read(files);
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }
        List<EmptyPair> pairs = makePairs(tables);
        List<InclusionDependency> sols = new ArrayList<>();

        for(EmptyPair p: pairs) {
            //List<InclusionDependency> l = results.get(p);
            //List<InclusionDependency> temp = new ArrayList<>();

            InclusionDependency temp1 = new InclusionDependency(new File(p.getColumnFile1()), new String[]{p.getColumnName1()},
                        new File(p.getColumnFile2()), new String[]{p.getColumnName2()});
            InclusionDependency temp2 = new InclusionDependency(new File(p.getColumnFile2()), new String[]{p.getColumnName2()},
                        new File(p.getColumnFile1()), new String[]{p.getColumnName1()});


            if(solution.contains(temp1.toString())) {
                sols.add(temp1);
            }
            if(solution.contains(temp2.toString())) {
                sols.add(temp2);
            }

        }

        for(InclusionDependency id: sols){
            analyzeTask(tables, id, "only sols: ", solution);
        }
    }


    public static void main(String[] args) {
        analyzeSolutions();
    }
}
