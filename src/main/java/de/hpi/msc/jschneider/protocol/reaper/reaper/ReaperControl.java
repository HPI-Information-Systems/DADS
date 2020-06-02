package de.hpi.msc.jschneider.protocol.reaper.reaper;

import akka.actor.Terminated;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.heartbeat.HeartbeatEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperMessages;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.protocol.statistics.rootActor.StatisticsRootActorControl;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.SneakyThrows;
import lombok.val;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;

public class ReaperControl extends AbstractProtocolParticipantControl<ReaperModel>
{
    private static class CreateUtilizationEventMessage implements Serializable
    {
        private static final long serialVersionUID = -9154128760961107357L;
    }

    public ReaperControl(ReaperModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(ReaperMessages.WatchMeMessage.class, this::onWatchMe)
                    .match(Terminated.class, this::onTerminated)
                    .match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                    .match(CreateUtilizationEventMessage.class, this::measureUtilization)
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination)
                    .match(HeartbeatEvents.HeartbeatPanicEvent.class, this::onHeartbeatPanic);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
    }

    private void onWatchMe(ReaperMessages.WatchMeMessage message)
    {
        try
        {
            if (!ProcessorId.of(message.getSender()).equals(ProcessorId.of(getModel().getSelf())))
            {
                getLog().error("Actor of remote system ({}) wants to be watched!", ProcessorId.of(message.getSender()));
                return;
            }

            if (tryWatch(message.getSender()))
            {
                getLog().debug("{} created, now watching {} actors.",
                               message.getSender().path(),
                               getModel().getWatchedActors().size());
            }
        }
        finally
        {
            complete(message);
        }
    }

    @Override
    protected void onTerminated(Terminated message)
    {
        super.onTerminated(message);

        getLog().debug("{} terminated, now watching {} actors.",
                       message.getActor().path(),
                       getModel().getWatchedActors().size());

//        if (!getModel().getWatchedActors().isEmpty())
//        {
//            return;
//        }
//
//        tryTerminateActorSystem();
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);

            for (val participant : getModel().getProcessors())
            {
                subscribeToEvent(participant.getId(), ProtocolType.Heartbeat, HeartbeatEvents.HeartbeatPanicEvent.class);
            }

            startMeasuringUtilization();
        }
        finally
        {
            complete(message);
        }
    }

    private void startMeasuringUtilization()
    {
        if (!getLocalProtocol(ProtocolType.Statistics).isPresent())
        {
            return;
        }

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
                                                 () -> getModel().getSelf().tell(new CreateUtilizationEventMessage(), getModel().getSelf()),
                                                 dispatcher);
        getModel().setMeasureUtilizationTask(task);
    }

    private void measureUtilization(CreateUtilizationEventMessage message)
    {
        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.ActorSystemUtilizationEvent.builder()
                                                                                                             .sender(getModel().getSelf())
                                                                                                             .receiver(eventDispatcher)
                                                                                                             .dateTime(LocalDateTime.now())
                                                                                                             .numberOfActors(getModel().getWatchedActors().size())
                                                                                                             .build());
    }

    @SneakyThrows
    private void onReadyForTermination(ScoringEvents.ReadyForTerminationEvent message)
    {
        try
        {
            getLog().info("Received ready for termination event.");
            Thread.sleep(1000); // wait for all actors to gracefully shut down
            tryTerminateActorSystem();
        }
        finally
        {
            complete(message);
        }
    }

    private void onHeartbeatPanic(HeartbeatEvents.HeartbeatPanicEvent message)
    {
        try
        {
            getLog().error("{} GOT A HEARTBEAT-PANIC! SHUTTING DOWN!", ProcessorId.of(message.getSender()));
            tryTerminateActorSystem();
        }
        finally
        {
            complete(message);
        }
    }

    private void tryTerminateActorSystem()
    {
        try
        {
            if (getModel().getMeasureUtilizationTask() != null)
            {
                getModel().getMeasureUtilizationTask().cancel();
            }

            getLocalProtocol(ProtocolType.Reaper).ifPresent(protocol ->
                                                            {
                                                                send(ReaperEvents.ActorSystemReapedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(protocol.getEventDispatcher())
                                                                                                        .build());
                                                            });

            getModel().getTerminateActorSystemCallback().run();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to terminate actor system!", exception);
        }
    }
}
