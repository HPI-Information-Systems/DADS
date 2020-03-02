package de.hpi.msc.jschneider.utility.dataTransfer.distributor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@SuperBuilder
public class DataDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private ActorRef supervisor;
    @Getter
    private long operationId;
    @NonNull @Getter
    private DataSource dataSource;
    @NonNull @Getter
    private DataDistributorInitializer initializer;
    @Getter @Setter @Builder.Default
    private boolean finished = false;
    @Getter @Setter
    private DataTransferMessages.InitializeDataTransferMessage initializationMessage;
    @Getter @Setter
    private LocalDateTime startTime;
    @Getter @Setter
    private LocalDateTime endTime;
    @Getter @NonNull
    private final Counter transferredBytes = new Counter(0L);
}
