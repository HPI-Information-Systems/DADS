package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import de.hpi.msc.jschneider.actor.utility.actorPool.ActorPool;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableLong;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageSenderProxyModel extends AbstractActorModel
{
    @NonNull @Getter
    private final Map<RootActorPath, ActorPool> remoteMessageReceivers = new HashMap<>();
    @NonNull @Getter
    private final Map<RootActorPath, MutableLong> sentMessageCounter = new HashMap<>();
}
