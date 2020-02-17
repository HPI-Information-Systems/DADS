package de.hpi.msc.jschneider.protocol.reaper.reaper;

import akka.actor.Terminated;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperMessages;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class ReaperControl extends AbstractProtocolParticipantControl<ReaperModel>
{
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
                    .match(ScoringEvents.ReadyForTerminationEvent.class, this::onReadyForTermination);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
    }

    private void onWatchMe(ReaperMessages.WatchMeMessage message)
    {
        try
        {
            if (message.getSender().path().root() != getModel().getSelf().path().root())
            {
                getLog().error(String.format("Actor of remote system (%1$s) wants to be watched!", message.getSender().path().root()));
                return;
            }

            if (tryWatch(message.getSender()))
            {
                getLog().debug(String.format("%1$s is now watching %2$d actors.",
                                             getClass().getName(),
                                             getModel().getWatchedActors().size()));
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void onTerminated(Terminated message)
    {
        if (!tryUnwatch(message.getActor()))
        {
            return;
        }

        if (!getModel().getWatchedActors().isEmpty())
        {
            return;
        }

        tryTerminateActorSystem();
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        try
        {
            subscribeToMasterEvent(ProtocolType.Scoring, ScoringEvents.ReadyForTerminationEvent.class);
        }
        finally
        {
            complete(message);
        }
    }

    private void onReadyForTermination(ScoringEvents.ReadyForTerminationEvent message)
    {
        try
        {
            getLog().info("Received ready for termination event.");
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
