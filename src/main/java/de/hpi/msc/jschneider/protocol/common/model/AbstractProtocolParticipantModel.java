package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

@SuperBuilder
public abstract class AbstractProtocolParticipantModel implements ProtocolParticipantModel
{
    @NonNull @Getter
    private final Set<ActorRef> watchedActors;
    @NonNull @Getter
    private final Set<ActorRef> childActors = new HashSet<>();
    private Logger log;
    @Setter
    private Callable<ActorRef> selfProvider;
    @Setter
    private Callable<ActorRef> senderProvider;
    @Getter @Setter
    private Consumer<ActorRef> watchActorCallback;
    @Getter @Setter
    private Consumer<ActorRef> unwatchActorCallback;
    @Getter @Setter
    private Function<Props, ActorRef> childFactory;

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
}
