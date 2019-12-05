package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

@SuperBuilder
public abstract class AbstractProtocolParticipantModel implements ProtocolParticipantModel
{
    private Logger log;
    @NonNull @Getter
    private final Set<ActorRef> watchedActors = new HashSet<>();
    @NonNull @Getter
    private final Set<ActorRef> childActors = new HashSet<>();
    @Setter
    private Callable<ActorRef> selfProvider;
    @Setter
    private Callable<ActorRef> senderProvider;
    @Setter
    private Callable<Processor[]> processorProvider;
    @Getter @Setter
    private Consumer<ActorRef> watchActorCallback;
    @Getter @Setter
    private Consumer<ActorRef> unwatchActorCallback;
    @Getter @Setter
    private ActorFactory childFactory;

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    @Override
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

    @Override
    public final Processor getLocalProcessor()
    {
        return getProcessor(getSelf().path().root()).get();
    }

    @Override
    public final Optional<Processor> getMasterProcessor()
    {
        try
        {
            for (val processor : processorProvider.call())
            {
                if (processor.isMaster())
                {
                    return Optional.of(processor);
                }
            }

            return Optional.empty();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve master processor!", exception);
            return Optional.empty();
        }
    }

    @Override
    public final Optional<Processor> getProcessor(RootActorPath actorSystem)
    {
        try
        {
            for (val processor : processorProvider.call())
            {
                if (processor.getRootPath().equals(actorSystem))
                {
                    return Optional.of(processor);
                }
            }

            return Optional.empty();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve (remote) processor!", exception);
            return Optional.empty();
        }
    }

    @Override
    public int getNumberOfProcessors()
    {
        try
        {
            return processorProvider.call().length;
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve number of processors!", exception);
            return 0;
        }
    }

    @Override
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
