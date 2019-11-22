package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import lombok.val;

public class ReaperControl extends AbstractActorControl<ReaperModel>
{
    public ReaperControl(ReaperModel model)
    {
        super(model);
    }

    @Override
    public void onTerminated(Terminated message)
    {
        super.onTerminated(message);

        if (!getWatchedActors().isEmpty())
        {
            return;
        }

        try
        {
            getModel().getTerminateSystemCallback().run();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to terminate actor system!", exception);
        }
    }

    public void onWatchMe(ReaperMessages.WatchMeMessage message)
    {
        try
        {
            val sender = message.getSender();
            if (sender.path().root() != getSelf().path().root())
            {
                getLog().error(String.format("Remote actor (%1$s) wants to be watched by %2$s!", sender.path(), getClass().getName()));
                return;
            }

            if (!watch(sender))
            {
                return;
            }

            getLog().debug(String.format("%1$s now watches %2$s actors.", getClass().getName(), getWatchedActors().size()));
        }
        finally
        {
            complete(message);
        }
    }
}
