package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.helper.*;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		Task task;
	}

	@Getter
	@AllArgsConstructor
	public static abstract class Task {
		abstract DependencyMiner.Result handle();
	}

	//public static class

	public static class AnalyzeTask extends Task{
		private final AnalyzePair analyzePair;
		AnalyzeTask(AnalyzePair analyzePair){
			this.analyzePair = analyzePair;
		}


		@Override
		DependencyMiner.Result handle() {

			DependencyMiner.Result result = new DependencyMiner.Result();
			result.setEmptyPair(analyzePair.toEmpty());
			result.setHasResult(true);


			InclusionDependency id1 = analyzePair.firstIsSubSetToSecond();
			if(id1 != null) {
				if(!result.getInclusionDependencies().contains(id1)) {
					result.getInclusionDependencies().add(id1);
				}
			}
			InclusionDependency id2 = analyzePair.secondIsSubSetToFirst();
			if(id2 != null) {
				if(!result.getInclusionDependencies().contains(id2)) {
					result.getInclusionDependencies().add(id2);
				}
			}
			if(id1  == null&& id2==null){
				if(!result.getInclusionDependencies().contains(null)) {
					result.getInclusionDependencies().add(null);
				}
			}
			return result;
		}
	}

	public static class CreateTableTask extends Task{
		private final List<String[]> batch;
		private final String filepath;
		private final String[] header;

		CreateTableTask(String filepath,List<String[]> batch, String[] header){
			this.filepath = filepath;
			this.batch = batch;
			this.header = header;
		}
		@Override
		DependencyMiner.Result handle() {
			DependencyMiner.Result result = new DependencyMiner.Result();
			result.table = new CSVTable(filepath, batch, header);
			result.table.split();
			return result;
		}
	}

	public static class MergeBatchColumn extends Task {
		String path;
		String cn;
		List<CSVColumn> columns;

		public MergeBatchColumn(String path, String cn, List<CSVColumn> columns) {
			this.path = path;
			this.cn = cn;
			this.columns = columns;
		}

		@Override
		DependencyMiner.Result handle() {

			DependencyMiner.Result result = new DependencyMiner.Result();
			ArrayList<String> entries = new ArrayList<>();
			for(CSVColumn c: columns){
				entries.addAll(c.getEntries());
			}
			result.column = new CSVColumn(path, cn, entries.toArray(new String[]{}));
			return result;
		}
	}


	/*

	public static class MakePairsTask extends Task {
		EmptyTable t1;
		EmptyTable t2;
		public MakePairsTask(EmptyTable t1, EmptyTable t2) {
			this.t1 = t1;
			this.t2 = t2;
		}

		@Override
		DependencyMiner.Result handle() {
			DependencyMiner.Result result = new DependencyMiner.Result();
			List<EmptyPair> pairs = new ArrayList<>();
			for(String col1: this.t1.getHeader()){
				for(String col2: this.t2.getHeader()){
					pairs.add(new EmptyPair(this.t1.getFilepath(),this.t2.getFilepath(), col1, col2));
				}
			}
			result.setPairs(pairs);
			return result;
		}
	}

	 */
	public static class WaitTask extends Task {
		@Override
		DependencyMiner.Result handle() {
			try {
				wait(1000);
			} catch (InterruptedException e) {
				//TODO: log
			}
			return new DependencyMiner.Result();
		}
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		// I should probably know how to solve this task, but for now I just pretend some work...


		//TODO: handle Task
		/*
		int result = message.getTask();
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime)
			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;

		 */

		sendResults(message,message.getTask().handle());

		//LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
		//this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private void sendResults(TaskMessage message, DependencyMiner.Result result){
		LargeMessageProxy.LargeMessage completionMessage =
				new DependencyMiner.CompletionMessage(
						this.getContext().getSelf(),
						result);
		this.largeMessageProxy.tell(
				new LargeMessageProxy.SendMessage(completionMessage,
				message.getDependencyMinerLargeMessageProxy()));
	}


}
