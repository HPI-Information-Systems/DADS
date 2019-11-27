package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractProtocolParticipantControl<TModel extends ProtocolParticipantModel> implements ProtocolParticipantControl<TModel>
{
    private Logger log;
    private TModel model;

    protected AbstractProtocolParticipantControl(TModel model)
    {
        setModel(model);
    }

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    @Override
    public final TModel getModel()
    {
        return model;
    }

    @Override
    public final void setModel(TModel model)
    {
        if (model == null)
        {
            throw new NullPointerException();
        }

        this.model = model;
    }

    public final boolean tryWatch(ActorRef subject)
    {
        if (subject == null || subject == ActorRef.noSender())
        {
            return false;
        }

        try
        {
            getModel().getWatchActorCallback().accept(subject);
            return getModel().getWatchedActors().add(subject);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to watch actor!", exception);
            return false;
        }
    }

    public final boolean tryUnwatch(ActorRef subject)
    {
        if (subject == null || subject == ActorRef.noSender())
        {
            return false;
        }

        try
        {
            getModel().getUnwatchActorCallback().accept(subject);
            return getModel().getWatchedActors().remove(subject);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to unwatch actor!", exception);
            return false;
        }
    }

    public final ActorRef spawnChild(Props props)
    {
        try
        {
            val child = getModel().getChildFactory().apply(props);
            if (!getModel().getChildActors().add(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to add newly created child! This should never happen!");
                return ActorRef.noSender();
            }

            if (!tryWatch(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to watch newly created child! This should never happen!");
                return ActorRef.noSender();
            }

            return child;
        }
        catch (Exception exception)
        {
            getLog().error("Unable to create a new child!", exception);
            return ActorRef.noSender();
        }
    }

    public void onAny(Object message)
    {
        getLog().warn(String.format("%1$s received unmatched message of type %2$s!",
                                    getClass().getName(),
                                    message.getClass().getName()));
    }
}
