package de.hpi.msc.jschneider.utility;

import akka.japi.pf.FI;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class ImprovedReceiveBuilderTest
{
    @Test
    public void latestMessageHandlerIsUsed()
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

    @Test
    public void latestMatchAnyIsUsed()
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
