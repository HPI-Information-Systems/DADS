package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

@SuperBuilder
public abstract class AbstractActorModel implements ActorModel
{
    private Logger log;
    @NonNull
    protected Callable<ActorRef> selfProvider;
    @NonNull
    protected Callable<ActorRef> senderProvider;
    @NonNull
    protected Callable<ActorRef> messageDispatcherProvider;

    @NonNull @Getter
    protected Function<Props, ActorRef> childFactory;
    @NonNull @Getter
    protected final Set<ActorRef> childActors = new HashSet<>();
    @NonNull @Getter
    protected final Set<ActorRef> watchedActors = new HashSet<>();
    @NonNull @Getter
    protected Consumer<ActorRef> watchActorCallback;

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    public final ActorRef getSelf()
    {
        try
        {
            return selfProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve self!", exception);
            return ActorRef.noSender();
        }
    }

    public final ActorRef getSender()
    {
        try
        {
            return senderProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve sender!", exception);
            return ActorRef.noSender();
        }
    }

    public final ActorRef getMessageDispatcher()
    {
        try
        {
            return messageDispatcherProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve messageSender!", exception);
            return ActorRef.noSender();
        }
    }
}
