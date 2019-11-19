package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.common.BaseActorController;
import de.hpi.msc.jschneider.actor.common.Postbox;

public class ReaperController extends BaseActorController
{
    public ReaperController(Postbox owner)
    {
        super(owner);
    }

    public void onWatchMe(ReaperMessages.WatchMeMessage message)
    {
        if (sender().path().root() != self().path().root())
        {
            log().warn("Actor of remote system tried to get watched!");
            return;
        }

        if (!watch(sender()))
        {
            return;
        }

        log().debug("{} is now watching {} actors.", getClass().getName(), watchedActors.size());
    }

    public void onTerminated(Terminated message)
    {
        if (!senderTerminated())
        {
            return;
        }

        log().debug("{} is now watching {} actors.", getClass().getName(), watchedActors.size());
    }
}
