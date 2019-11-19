package de.hpi.msc.jschneider.actor.common.messageReceiverProxy;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableLong;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageReceiverProxyModel extends AbstractActorModel
{
    @NonNull @Getter
    private final Map<RootActorPath, MutableLong> receivedMessageCounter = new HashMap<>();
}
