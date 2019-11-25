package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy.MessageProxyMessages;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public abstract class AbstractActorControl<TActorModel extends ActorModel> implements ActorControl<TActorModel>
{
    private Logger log;
    @Getter @Setter
    private TActorModel model;

    protected AbstractActorControl(TActorModel model)
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

    protected final ActorRef getSelf()
    {
        return getModel().getSelf();
    }

    protected final ActorRef getSender()
    {
        return getModel().getSender();
    }

    protected final ActorRef getMessageDispatcher()
    {
        return getModel().getMessageDispatcher();
    }

    protected final Set<ActorRef> getChildActors()
    {
        return getModel().getChildActors();
    }

    protected final Set<ActorRef> getWatchedActors()
    {
        return getModel().getWatchedActors();
    }

    protected final ActorRef createChild(Props props)
    {
        try
        {
            val child = getModel().getChildFactory().apply(props);
            if (!getChildActors().add(child))
            {
                child.tell(PoisonPill.getInstance(), getSender());
                getLog().error("Unable to add newly created child! This should never happen!");
                return ActorRef.noSender();
            }

            if (!watch(child))
            {
                child.tell(PoisonPill.getInstance(), getSender());
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

    protected final boolean watch(ActorRef actor)
    {
        if (getWatchedActors().contains(actor))
        {
            return false;
        }

        try
        {
            getModel().getWatchActorCallback().accept(actor);
            return getModel().getWatchedActors().add(actor);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to watch actor!", exception);
            return false;
        }
    }

    public void send(Message message)
    {
        getMessageDispatcher().tell(message, getSelf());
    }

    public void complete(Message message)
    {
        val completedMessage = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                           .sender(getSelf())
                                                                           .receiver(message.getSender())
                                                                           .acknowledgedMessageId(message.getId())
                                                                           .build();

        send(completedMessage);
    }

    public void onBackPressure(MessageProxyMessages.BackPressureMessage message)
    {
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException interruptedException)
        {
            getLog().warn(interruptedException);
        }
        finally
        {
            complete(message);
        }
    }

    public void onTerminated(Terminated message)
    {
        getWatchedActors().remove(getSender());
        getChildActors().remove(getSender());

        getLog().debug(String.format("%1$s has now %2$d children and watches %3$d total actors.",
                                     getClass().getName(),
                                     getChildActors().size(),
                                     getWatchedActors().size()));
    }

    public void onAny(Object message)
    {
        getLog().warn(String.format("%1$s does not handle messages of type %2$s.", getClass().getName(), message.getClass().getName()));

        if (message instanceof Message)
        {
            complete((Message) message);
        }
    }
}
