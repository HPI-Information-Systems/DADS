package de.hpi.msc.jschneider.actor.utility.actorPool;

import akka.actor.ActorRef;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class RoundRobinActorPool implements ActorPool
{
    private static final Logger Log = LogManager.getLogger(RoundRobinActorPool.class);

    private final ActorRef[] actors;
    private final AtomicInteger nextActorIndex = new AtomicInteger();

    public RoundRobinActorPool(ActorRef[] actors)
    {
        this.actors = actors;
    }

    public RoundRobinActorPool(String namePrefix, int size, Function<String, ActorRef> actorFactory)
    {
        actors = createActors(namePrefix, size, actorFactory);
    }

    private ActorRef[] createActors(String namePrefix, int size, Function<String, ActorRef> actorFactory)
    {
        try
        {
            val actors = new ActorRef[size];
            for (var i = 0; i < size; ++i)
            {
                actors[i] = actorFactory.apply(namePrefix + i);
            }

            return actors;
        }
        catch (Exception exception)
        {
            Log.error(String.format("Unable to create actors for pool %1$s", getClass().getName()), exception);
            return null;
        }
    }

    @Override
    public ActorRef[] getActors()
    {
        return actors;
    }

    @Override
    public ActorRef getActor()
    {
        return actors[nextActorIndex.getAndIncrement() % actors.length];
    }
}
