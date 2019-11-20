package de.hpi.msc.jschneider.utility;

import akka.japi.pf.FI;
import de.hpi.msc.jschneider.actor.utility.ImprovedReceiveBuilder;
import junit.framework.TestCase;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class ImprovedReceiveBuilderTest extends TestCase
{
    public void testLatestMessageHandlerIsUsed()
    {
        val builder = new ImprovedReceiveBuilder();

        val numberOfCallsToHandler1 = new AtomicInteger();
        FI.UnitApply<String> handler1 = argument -> numberOfCallsToHandler1.getAndIncrement();

        val numberOfCallsToHandler2 = new AtomicInteger();
        FI.UnitApply<String> handler2 = argument -> numberOfCallsToHandler2.getAndIncrement();

        val receive1 = builder.match(String.class, handler1).build();
        receive1.onMessage().apply("1");
        assertThat(numberOfCallsToHandler1.get()).isEqualTo(1);
        assertThat(numberOfCallsToHandler2.get()).isEqualTo(0);

        val receive2 = builder.match(String.class, handler2).build();
        receive2.onMessage().apply("2");
        assertThat(numberOfCallsToHandler1.get()).isEqualTo(1);
        assertThat(numberOfCallsToHandler2.get()).isEqualTo(1);
    }

    public void testLatestMatchAnyIsUsed()
    {
        val builder = new ImprovedReceiveBuilder();

        val numberOfCallsToHandler1 = new AtomicInteger();
        FI.UnitApply<Object> handler1 = argument -> numberOfCallsToHandler1.getAndIncrement();

        val numberOfCallsToHandler2 = new AtomicInteger();
        FI.UnitApply<Object> handler2 = argument -> numberOfCallsToHandler2.getAndIncrement();

        val receive1 = builder.matchAny(handler1).build();
        receive1.onMessage().apply("1");
        assertThat(numberOfCallsToHandler1.get()).isEqualTo(1);
        assertThat(numberOfCallsToHandler2.get()).isEqualTo(0);

        val receive2 = builder.matchAny(handler2).build();
        receive2.onMessage().apply("2");
        assertThat(numberOfCallsToHandler1.get()).isEqualTo(1);
        assertThat(numberOfCallsToHandler2.get()).isEqualTo(1);
    }
}
