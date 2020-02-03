package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPCACalculatorControl extends ProtocolTestCase
{
    private TestProcessor remoteProcessor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        remoteProcessor = createSlave();
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.PrincipalComponentAnalysis, ProtocolType.SequenceSliceDistribution};
    }

    private PCACalculatorModel dummyModel()
    {
        return finalizeModel(PCACalculatorModel.builder()
                                               .build());
    }

    private PCACalculatorControl control()
    {
        return new PCACalculatorControl(dummyModel());
    }

    private PCAMessages.InitializePCACalculationMessage initializeCalculation(PCACalculatorControl control, PartialFunction<Object, BoxedUnit> messageInterface, TestProcessor... processors)
    {
        val processorIndices = new HashMap<Long, ProcessorId>();
        var myProcessorIndex = 0L;
        for (var i = 0L; i < (long) processors.length; ++i)
        {
            processorIndices.put(i, processors[(int) i].getId());

            if (processors[(int) i] == localProcessor)
            {
                myProcessorIndex = i;
            }
        }

        val message = PCAMessages.InitializePCACalculationMessage.builder()
                                                                 .sender(remoteProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref())
                                                                 .receiver(self.ref())
                                                                 .processorIndices(processorIndices)
                                                                 .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getMyProcessorIndex()).isEqualTo(myProcessorIndex);
        assertThat(control.getModel().getProcessorIndices()).isEqualTo(processorIndices);

        return message;
    }

    private MatrixStore<Double> simpleMatrix()
    {
        return (new MatrixInitializer(3))
                .appendRow(new double[]{0.0d, 1.0d, 2.0d})
                .appendRow(new double[]{3.0d, 4.0d, 5.0d})
                .appendRow(new double[]{6.0d, 7.0d, 8.0d})
                .create();
    }

    public void testLocalProjectionCreated()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val projection = simpleMatrix();

        val event = SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                          .sender(localProcessor.getProtocolRootActor(ProtocolType.SequenceSliceDistribution).ref())
                                                                          .receiver(self.ref())
                                                                          .firstSubSequenceIndex(0L)
                                                                          .projection(projection)
                                                                          .build();
        messageInterface.apply(event);

        assertThat(control.getModel().getProjection()).isEqualTo(projection);
        assertThat(control.getModel().getCurrentCalculationStep().get()).isEqualTo(0L);

        assertThatMessageIsCompleted(event);
    }

    public void testInitializeCalculation()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val processorIndices = new HashMap<Long, ProcessorId>();
        processorIndices.put(0L, remoteProcessor.getId());
        processorIndices.put(1L, localProcessor.getId());

        val message = PCAMessages.InitializePCACalculationMessage.builder()
                                                                 .sender(remoteProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref())
                                                                 .receiver(self.ref())
                                                                 .processorIndices(processorIndices)
                                                                 .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getMyProcessorIndex()).isEqualTo(1L);
        assertThat(control.getModel().getProcessorIndices()).isEqualTo(processorIndices);
        assertThat(control.getModel().getCurrentCalculationStep().get()).isEqualTo(0L);

        assertThatMessageIsCompleted(message);
    }

    public void testStartCalculation()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);
        val projection = simpleMatrix();

        val initializationMessage = initializeCalculation(control, messageInterface, remoteProcessor, localProcessor);
        assertThatMessageIsCompleted(initializationMessage);

        val event = SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                          .sender(localProcessor.getProtocolRootActor(ProtocolType.SequenceSliceDistribution).ref())
                                                                          .receiver(self.ref())
                                                                          .firstSubSequenceIndex(0L)
                                                                          .projection(projection)
                                                                          .build();
        messageInterface.apply(event);

        val initializeColumnMeansTransferMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializeColumnMeansTransferMessage.class);
        assertThat(initializeColumnMeansTransferMessage.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref());
        assertThat(initializeColumnMeansTransferMessage.getProcessorIndex()).isEqualTo(1L);
        assertThat(initializeColumnMeansTransferMessage.getNumberOfRows()).isEqualTo(projection.countRows());

        val initializeRTransferMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializeRTransferMessage.class);
        assertThat(initializeRTransferMessage.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref());
        assertThat(initializeRTransferMessage.getProcessorIndex()).isEqualTo(1L);
        assertThat(initializeRTransferMessage.getCurrentStepNumber()).isEqualTo(0L);

        assertThatMessageIsCompleted(event);

        assertThat(control.getModel().getLocalR()).isNotNull();
        assertThat(control.getModel().getCurrentCalculationStep().get()).isEqualTo(1L);
    }
}
