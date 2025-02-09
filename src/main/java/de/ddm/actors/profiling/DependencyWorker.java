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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	private static final Log log = new SimpleLog("DependencyWorker");

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
		public abstract DependencyMiner.Result handle();
		public abstract void load(MinerManager.TaskLoader loader);
	}

	//public static class

	public static class AnalyzeTask extends Task{
		private AnalyzePair analyzePair;
		private final EmptyPair emptyPair;

		public AnalyzeTask(EmptyPair emptyPair){
			this.emptyPair = emptyPair;
		}


		@Override
		public DependencyMiner.Result handle() {
			log.info("AnalyzeTask: handle");

			DependencyMiner.Result result = new DependencyMiner.Result();
			result.setHasResult(true);


			/*
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

			 */
			result.getInclusionDependencies().addAll(analyzePair.getInclusionDependency());
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			analyzePair = loader.getAnalyzerPair(emptyPair);
		}
	}

	@AllArgsConstructor
	public static class CreateTableTask extends Task{
		private final List<String[]> batch;
		private final String filepath;
		private final String[] header;

		@Override
		public DependencyMiner.Result handle() {
			log.info("CreateTableTask: handle");
			DependencyMiner.Result result = new DependencyMiner.Result();
			result.table = new CSVTable(filepath, batch, header);
			result.table.split();
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			//do nothing
		}
	}

	public static class MergeBatchColumn extends Task {
		String path;
		String cn;
		List<CSVColumn> columns;

		public MergeBatchColumn(String path, String cn) {
			this.path = path;
			this.cn = cn;
		}

		@Override
		public DependencyMiner.Result handle() {
			log.info("MergeBatchColumn: handle");

			DependencyMiner.Result result = new DependencyMiner.Result();
			ArrayList<String> entries = new ArrayList<>();
			for(CSVColumn c: columns){
				entries.addAll(c.getEntries());
			}
			result.column = new CSVColumn(path, cn, entries);
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			this.columns = loader.getColumns(path, cn);
		}


	}


	@AllArgsConstructor
	public static class FilterInclusionDependencies extends Task{

		List<InclusionDependency> ids;

		@Override
		public DependencyMiner.Result handle() {
			log.info("FilterInclusionDependencies: handle");
			DependencyMiner.Result result = new DependencyMiner.Result();
			result.hasFilteredResult = true;
			result.setFilteredInclusionDependencies(InclusionDependencyFilter.filter(ids));
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			// do nothing
		}
	}


	public static class LastFilter extends Task{
		List<InclusionDependency> ids;
		@Override
		public DependencyMiner.Result handle() {
			log.info("LastFilter: handle");
			DependencyMiner.Result result = new DependencyMiner.Result();
			result.hasFilteredResult=true;
			result.setFilteredInclusionDependencies(InclusionDependencyFilter.getMore(ids));
			result.setLast(true);
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			//do nothing
			ids = loader.getAllResults();
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
		public DependencyMiner.Result handle() {
			log.info("WaitTask: handle");
			DependencyMiner.Result result = new DependencyMiner.Result();
			try {
				wait(1000);
			} catch (InterruptedException e) {
				//TODO: logWaitTask
				log.error("WaitTask: handle",e);
			}
			result.setWaited(true);
			return result;
		}

		@Override
		public void load(MinerManager.TaskLoader loader) {
			//do nothing
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


		//TO DO: handle Task
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
