package de.hpi.msc.jschneider.actor.common;

import de.hpi.msc.jschneider.actor.common.reaper.Reaper;
import de.hpi.msc.jschneider.actor.common.reaper.ReaperMessages;

public abstract class AbstractReapedActor<TActorModel extends ActorModel, TActorControl extends ActorControl<TActorModel>> extends AbstractActor<TActorModel, TActorControl>
{
    @Override
    public void preStart() throws Exception
    {
        super.preStart();

        control().send(ReaperMessages.WatchMeMessage.builder()
                                                    .sender(getSelf())
                                                    .receiver(Reaper.globalInstance())
                                                    .build());
    }
}
