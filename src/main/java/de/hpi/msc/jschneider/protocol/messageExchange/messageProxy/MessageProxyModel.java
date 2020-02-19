package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Scheduler;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import scala.concurrent.ExecutionContextExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@SuperBuilder
public class MessageProxyModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Counter totalNumberOfEnqueuedMessages = new Counter(0L);
    @NonNull @Getter
    private final Map<ActorPath, ActorMessageQueue> messageQueues = new HashMap<>();
    @NonNull @Getter
    private ActorRef remoteMessageDispatcher;
    @Getter @Setter @Builder.Default
    private int singleQueueBackPressureThreshold = 1000;
    @Getter @Setter @Builder.Default
    private int totalQueueBackPressureThreshold = 10000;
    @NonNull
    private Callable<Scheduler> schedulerProvider;
    @NonNull
    private Callable<ExecutionContextExecutor> dispatcherProvider;
    @Setter @Getter
    private Cancellable measureUtilizationTask;

    public final Scheduler getScheduler()
    {
        try
        {
            return schedulerProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve Scheduler!", exception);
            return null;
        }
    }

    public final ExecutionContextExecutor getDispatcher()
    {
        try
        {
            return dispatcherProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve Dispatcher!", exception);
            return null;
        }
    }
}
