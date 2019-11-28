package de.hpi.msc.jschneider.protocol.reaper.reaper;

import akka.actor.Terminated;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.messageExchange.AbstractMessageExchangeParticipantControl;
import de.hpi.msc.jschneider.protocol.reaper.ReaperEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class ReaperControl extends AbstractMessageExchangeParticipantControl<ReaperModel>
{
    public ReaperControl(ReaperModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(ReaperMessages.WatchMeMessage.class, this::onWatchMe)
                      .match(Terminated.class, this::onTerminated);
    }

    private void onWatchMe(ReaperMessages.WatchMeMessage message)
    {
        try
        {
            if (message.getSender().path().root() != getModel().getSelf().path().root())
            {
                getLog().error("Actor of remote system (%1$s) wants to be watched!");
                return;
            }

            if (tryWatch(message.getSender()))
            {
                getLog().info(String.format("%1$s is now watching %2$d actors.",
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

    private void tryTerminateActorSystem()
    {
        try
        {
            getLocalProtocol(ProtocolType.Reaper).ifPresent(protocol ->
                                                            {
                                                                send(ReaperEvents.ActorSystemReapedEvents.builder()
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
