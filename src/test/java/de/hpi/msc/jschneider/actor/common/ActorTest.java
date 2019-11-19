package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorSystem;
import junit.framework.TestCase;
import lombok.val;

import java.util.HashSet;
import java.util.Set;

public abstract class ActorTest extends TestCase
{
    protected ActorSystem actorSystem;
    private Set<ActorSystem> actorSystems = new HashSet<>();

    @Override
    protected void setUp()
    {
        actorSystem = createActorSystem();
    }

    protected ActorSystem createActorSystem()
    {
        val system = ActorSystem.create();
        actorSystems.add(system);

        return system;
    }

    @Override
    protected void tearDown()
    {
        for (val system : actorSystems)
        {
            system.terminate();
        }

        actorSystems.clear();
    }
}
