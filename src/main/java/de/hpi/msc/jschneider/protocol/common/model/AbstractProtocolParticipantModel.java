package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferManager;
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
    private Callable<Processor[]> processorProvider;
    @Setter
    private Callable<Long> maximumMessageSizeProvider;
    @Getter @Setter
    private Consumer<ActorRef> watchActorCallback;
    @Getter @Setter
    private Consumer<ActorRef> unwatchActorCallback;
    @Getter @Setter
    private ActorFactory childFactory;
    @Getter @Setter
    private DataTransferManager dataTransferManager;

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
    public final Optional<Processor> getProcessor(ActorRef actorRef)
    {
        return getProcessor(ProcessorId.of(actorRef));
    }

    @Override
    public final Optional<Processor> getProcessor(RootActorPath actorSystem)
    {
        return getProcessor(ProcessorId.of(actorSystem));
    }

    @Override
    public final Optional<Processor> getProcessor(ProcessorId processorId)
    {
        for (val processor : getProcessors())
        {
            if (processor.getId().equals(processorId))
            {
                return Optional.of(processor);
            }
        }

        return Optional.empty();
    }

    @Override
    public int getNumberOfProcessors()
    {
        return getProcessors().length;
    }

    @Override
    public Processor[] getProcessors()
    {
        try
        {
            return processorProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve processors!", exception);
            return new Processor[0];
        }
    }

    @Override
    public long getMaximumMessageSize()
    {
        try
        {
            return maximumMessageSizeProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve maximum message size!", exception);
            return 0L;
        }
    }
}
