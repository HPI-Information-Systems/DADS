package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorSystem;
import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestProcessor implements Processor
{
    @Setter @Getter
    private boolean isMaster;
    @Setter @Getter
    private ActorSystem actorSystem;
    @Setter
    private Set<Protocol> protocols = new HashSet<>();
    @Getter
    private final Map<String, TestProbe> actors = new HashMap<>();

    public static TestProcessor create(String actorSystemName, Protocol... protocols)
    {
        val actorSystem = ActorSystem.create(actorSystemName);
        val processor = new TestProcessor();

        processor.setMaster(false);
        processor.setActorSystem(actorSystem);
        processor.setProtocols(new HashSet<>(Arrays.asList(protocols)));

        return processor;
    }

    @Override
    public RootActorPath getRootPath()
    {
        return new RootActorPath(actorSystem.provider().getDefaultAddress(), "/");
    }

    public Protocol getProtocol(ProtocolType type)
    {
        for (val protocol : protocols)
        {
            if (protocol.getType() == type)
            {
                return protocol;
            }
        }

        return null;
    }

    public TestProbe getProtocolRootActor(ProtocolType type)
    {
        return ((TestProtocol) getProtocol(type)).getRootActorTestProbe();
    }

    public void addProtocol(Protocol protocol)
    {
        protocols.add(protocol);
    }

    @Override
    public Protocol[] getProtocols()
    {
        return protocols.toArray(new Protocol[0]);
    }

    public TestProbe createActor(String name)
    {
        val actor = TestProbe.apply(name, actorSystem);
        actors.put(name, actor);

        return actor;
    }

    @Override
    public long getMaximumMemoryInBytes()
    {
        return 1024 * 1024;
    }

    @Override
    public int getNumberOfThreads()
    {
        return 1;
    }

    public void terminate()
    {
        actorSystem.terminate();
    }
}
