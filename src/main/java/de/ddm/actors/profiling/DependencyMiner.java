package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
//import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.helper.*;
//import de.ddm.serialization.AkkaSerializable;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.File;
import java.util.*;
import java.util.stream.IntStream;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		Result result;
	}

	////////////////////////
	// Result Format      //
	////////////////////////



	@Getter
	@Setter
	@NoArgsConstructor
	public static class Result{
		boolean waited = false;
		boolean hasFilteredResult = false;
		List<InclusionDependency> filteredInclusionDependencies = new ArrayList<>();
		boolean hasResult = false;
		List<InclusionDependency> inclusionDependencies = new ArrayList<>();
		CSVTable table = null;
		CSVColumn column = null;
		boolean last = false;
		//List<EmptyPair> pairs = null;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));

		this.minerManager = new MinerManager();
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;


	//private final List<EmptyPair> pairs = new ArrayList<>();

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;






	private final MinerManager minerManager;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		if (message.getBatch().size() != 0) {
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));

			this.minerManager.addTask(new DependencyWorker.CreateTableTask(
					message.batch,
					inputFiles[message.id].getPath(),
					this.headerLines[message.id]));


		}else{
			minerManager.readTableFinish(inputFiles[message.id].getPath());
		}




		//TO DO: Handle batch
		// give batch to dependency worker
		// problem only received part of one table




		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, nextTask()));
			this.nextTask(dependencyWorker);
		}
		this.checkAllWorkersRunning();
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		//TODO: receive results
		/*
		if (this.headerLines[0] != null) {
			Random random = new Random();
			int dependent = random.nextInt(this.inputFiles.length);
			int referenced = random.nextInt(this.inputFiles.length);
			File dependentFile = this.inputFiles[dependent];
			File referencedFile = this.inputFiles[referenced];
			String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)], this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
			String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)], this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		 */
		if(message.getResult() != null) {
			if(message.getResult().hasFilteredResult){
				minerManager.handleFilteredResult(message.getResult().filteredInclusionDependencies);
				if (message.getResult().filteredInclusionDependencies != null){
					if (!message.getResult().filteredInclusionDependencies.isEmpty()) {
						this.resultCollector.tell(new ResultCollector.ResultMessage(message.getResult().filteredInclusionDependencies));
					}
				}
			}
			else if(message.getResult().isHasResult()) {
				minerManager.handleResults(message.getResult().getInclusionDependencies());

			}
			else if(message.getResult().getColumn() != null){
				minerManager.handleColumn(message.getResult().getColumn());
			}
			else if(message.getResult().getTable() != null){
				minerManager.handleTable(message.getResult().getTable());
			}

			if(message.getResult().isLast()){
				minerManager.lastDone();
			}
		}
		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
		//TO DO:  start next Task

		//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,  nextTask()));
		this.nextTask(dependencyWorker);

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (System.currentTimeMillis() - this.startTime > 2000000)
			this.end();
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		int i = this.dependencyWorkers.indexOf(dependencyWorker);
		minerManager.workerStopped(i);
		//this.tasks.add(this.currentlyDoing.get(i));
		//this.currentlyDoing.remove(i);
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}

	public void nextTask(ActorRef<DependencyWorker.Message> dependencyWorker){
		DependencyWorker.Task task = minerManager.nextTask();
		//this.currentlyDoing
		dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, task));
		minerManager.putCurrently(this.dependencyWorkers.indexOf(dependencyWorker), task);
	}

	public void checkAllWorkersRunning(){
		if(this.dependencyWorkers.size() != minerManager.workingSize()){
			/*
			IntStream.range(start, end)
					.boxed()
					.collect(Collectors.toList());

			 */
			IntStream.range(0, this.dependencyWorkers.size())
					.boxed()
					.filter(e->!(minerManager.isCurrentlyWorking(e)))
					.map(this.dependencyWorkers::get)
					.peek(this::nextTask);
		}
	}








}