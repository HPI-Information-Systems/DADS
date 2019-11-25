package de.hpi.msc.jschneider.actor;

import akka.testkit.TestProbe;
import junit.framework.TestCase;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public abstract class ActorTestCase extends TestCase
{
    private final List<TestNode> testNodes = new ArrayList<>();
    protected TestNode localNode;
    protected TestProbe self;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localNode = spawnNode("local");
        self = TestProbe.apply("self", localNode.getActorSystem());
    }

    protected TestNode spawnNode(String name)
    {
        val node = TestNode.create(name);
        testNodes.add(node);

        return node;
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        for (val node : testNodes)
        {
            node.terminate();
        }
    }
}
