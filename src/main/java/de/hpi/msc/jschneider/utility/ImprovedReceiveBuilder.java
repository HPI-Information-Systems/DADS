package de.hpi.msc.jschneider.utility;

import akka.actor.AbstractActor;
import akka.japi.pf.CaseStatement;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;
import java.util.Map;

public class ImprovedReceiveBuilder
{
    private static final PartialFunction<Object, BoxedUnit> EMPTY_CASE = CaseStatement.empty();

    private final Map<Class<?>, FI.UnitApply<?>> messageHandlers = new HashMap<>();

    public <P> ImprovedReceiveBuilder match(final Class<P> messageType, final FI.UnitApply<P> messageHandler)
    {
        messageHandlers.put(messageType, messageHandler);
        return this;
    }

    public ImprovedReceiveBuilder matchAny(final FI.UnitApply<Object> messageHandler)
    {
        return match(Object.class, messageHandler);
    }

    public AbstractActor.Receive build()
    {
        if (messageHandlers.isEmpty())
        {
            return new AbstractActor.Receive(EMPTY_CASE);
        }

        val receiveBuilder = ReceiveBuilder.create();
        for (val messageHandler : messageHandlers.entrySet())
        {
            receiveBuilder.matchUnchecked(messageHandler.getKey(), messageHandler.getValue());
        }

        return receiveBuilder.build();
    }
}
