package de.ddm.actors.profiling.analyzing;

import akka.actor.typed.javadsl.ActorContext;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.ResultCollector;
import de.ddm.helper.AnalyzePair;
import lombok.Setter;

public class MyAnalyzePairWorker extends DependencyWorker {

    private MyAnalyzePairWorker(ActorContext<Message> context){
        super(context);
        //super(context);
    }

    @Setter
    private AnalyzePair analyzePair;
    @Setter
    private ResultCollector collector;

    private int done = 0;

    public static MyAnalyzePairWorker create(ActorContext<Message> context,
                                             AnalyzePair analyzePair,
                                             ResultCollector collector){
        MyAnalyzePairWorker res = new MyAnalyzePairWorker(context);
        res.setAnalyzePair(analyzePair);
        res.setCollector(collector);
        return res;
    }

    private void close(){
        //close
        //put this worker in trash
    }


    private void sendResult(boolean res){
        //send

        this.close();
    }



    private void analyse(){
        if(done == 0){
            if(this.analyzePair.firstIsSubSetToSecond()){
                this.sendResult(true);
            }
            done ++;
        } else if (done>0) {
            if(this.analyzePair.secondIsSubSetToFirst()){
                this.sendResult(true);
            }
            this.sendResult(false);
        }
    }


}
