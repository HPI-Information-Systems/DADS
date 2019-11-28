package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

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
    public final Optional<Protocol> getLocalProtocol(ProtocolType protocolType)
    {
        return getProtocol(getModel().getSelf().path().root(), protocolType);
    }

    @Override
    public final Optional<Protocol> getProtocol(RootActorPath actorSystem, ProtocolType protocolType)
    {
        val processor = getModel().getProcessor(actorSystem);
        if (processor == null)
        {
            return Optional.empty();
        }

        for (val protocol : processor.getProtocols())
        {
            if (protocol.getType() == protocolType)
            {
                return Optional.of(protocol);
            }
        }

        return Optional.empty();
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

    public final Optional<ActorRef> trySpawnChild(Props props)
    {
        try
        {
            val child = getModel().getChildFactory().apply(props);
            if (!getModel().getChildActors().add(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to add newly created child! This should never happen!");
                return Optional.empty();
            }

            if (!tryWatch(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to watch newly created child! This should never happen!");
                return Optional.empty();
            }

            return Optional.ofNullable(child);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to create a new child!", exception);
            return Optional.empty();
        }
    }

    public void onAny(Object message)
    {
        getLog().warn(String.format("%1$s received unmatched message of type %2$s!",
                                    getClass().getName(),
                                    message.getClass().getName()));
    }
}
