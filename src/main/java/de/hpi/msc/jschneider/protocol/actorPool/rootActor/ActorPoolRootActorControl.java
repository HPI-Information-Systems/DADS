package de.hpi.msc.jschneider.protocol.actorPool.rootActor;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolEvents;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerModel;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;

public class ActorPoolRootActorControl extends AbstractProtocolParticipantControl<ActorPoolRootActorModel>
{
    private static class CreateUtilizationMeasurement implements Serializable
    {
        private static final long serialVersionUID = 6907499696399788226L;
    }

    public ActorPoolRootActorControl(ActorPoolRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(CreateUtilizationMeasurement.class, this::measureUtilization)
                    .match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination)
                    .match(ActorPoolMessages.WorkMessage.class, this::onWork)
                    .match(ActorPoolMessages.ExecuteDistributedFromFactoryMessage.class, this::onWorkFactory)
                    .match(ActorPoolMessages.WorkDoneMessage.class, this::onWorkDone);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        for (var workerNumber = 0; workerNumber < getModel().getMaximumNumberOfWorkers(); ++workerNumber)
        {
            spawnWorker();
        }

        if (StatisticsProtocol.IS_ENABLED)
        {
            subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        }
    }

    private void spawnWorker()
    {
        val model = ActorPoolWorkerModel.builder()
                                        .supervisor(getModel().getSelf())
                                        .build();
        val control = new ActorPoolWorkerControl(model);
        val worker = trySpawnChild(ProtocolParticipant.props(control), "ActorPoolWorker");

        if (!worker.isPresent())
        {
            getLog().error("Unable to spawn new ActorPoolWorker!");
            return;
        }

        getModel().getWorkers().add(worker.get());
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
            startMeasuringUtilization();
        }
        finally
        {
            complete(message);
        }
    }

    private void startMeasuringUtilization()
    {
        if (getModel().getMeasureUtilizationTask() != null)
        {
            getModel().getMeasureUtilizationTask().cancel();
        }

        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        assert scheduler != null : "Scheduler must not be null!";
        assert dispatcher != null : "Dispatcher must not be null!";

        val task = scheduler.scheduleAtFixedRate(Duration.ZERO,
                                                 StatisticsProtocol.MEASUREMENT_INTERVAL,
                                                 () -> getModel().getSelf().tell(new CreateUtilizationMeasurement(), getModel().getSelf()),
                                                 dispatcher);
        getModel().setMeasureUtilizationTask(task);
    }

    private void measureUtilization(CreateUtilizationMeasurement message)
    {
        trySendEvent(ProtocolType.ActorPool, eventDispatcher -> ActorPoolEvents.UtilizationEvent.builder()
                                                                                                .sender(getModel().getSelf())
                                                                                                .receiver(eventDispatcher)
                                                                                                .dateTime(LocalDateTime.now())
                                                                                                .numberOfWorkers(getModel().getMaximumNumberOfWorkers())
                                                                                                .numberOfAvailableWorkers(getModel().getWorkers().size())
                                                                                                .workQueueSize(getModel().getWorkMessages().size())
                                                                                                .build());
    }

    private void onReadyForTermination(ScoringEvents.ReadyForTerminationEvent message)
    {
        try
        {
            if (getModel().getMeasureUtilizationTask() != null)
            {
                getModel().getMeasureUtilizationTask().cancel();
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void onWork(ActorPoolMessages.WorkMessage message)
    {
        try
        {
            getModel().getWorkMessages().add(message);
            dispatchWork();
        }
        finally
        {
            complete(message);
        }
    }

    private void onWorkFactory(ActorPoolMessages.ExecuteDistributedFromFactoryMessage message)
    {
        try
        {
            while (message.getWorkFactory().hasNext())
            {
                getModel().getWorkMessages().add(message.getWorkFactory().next(getModel().getSelf()));
            }
            dispatchWork();
        }
        finally
        {
            complete(message);
        }
    }

    private void onWorkDone(ActorPoolMessages.WorkDoneMessage message)
    {
        try
        {
            getModel().getWorkers().add(message.getSender());
            dispatchWork();
        }
        finally
        {
            complete(message);
        }
    }

    private void dispatchWork()
    {
        while (!getModel().getWorkers().isEmpty() && !getModel().getWorkMessages().isEmpty())
        {
            workOnNextItem();
        }
    }

    private void workOnNextItem()
    {
        val work = getModel().getWorkMessages().poll();
        val worker = getModel().getWorkers().poll();

        assert work != null : "Work == null!";
        assert worker != null : "Worker == null!";

        send(work.redirectTo(worker));
    }
}
