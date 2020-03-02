package de.hpi.msc.jschneider.utility.dataTransfer.distributor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;

@FunctionalInterface
public interface DataDistributorInitializer
{
    DataTransferMessages.InitializeDataTransferMessage initialize(ActorRef dataDistributor, long operationId);
}
