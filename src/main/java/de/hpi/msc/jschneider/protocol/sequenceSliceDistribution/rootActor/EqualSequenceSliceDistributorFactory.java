package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

//public class EqualSequenceSliceDistributorFactory implements SequenceSliceDistributorFactory
//{
//    private final int expectedNumberOfProcessors;
//    private final SequenceReader sequenceReaderTemplate;
//    private final Map<RootActorPath, SequenceReader> sequenceReaders = new HashMap<>();
//    private long nextSequenceReaderStartIndex;
//
//    public EqualSequenceSliceDistributorFactory(int expectedNumberOfProcessors, SequenceReader sequenceReader)
//    {
//        this.expectedNumberOfProcessors = expectedNumberOfProcessors;
//        sequenceReaderTemplate = sequenceReader;
//    }
//
//    @Override
//    public Props createSequenceSliceDistributor(Processor processor)
//    {
//        var reader = sequenceReaders.get(processor.getRootPath());
//        if (reader == null)
//        {
//
//        }
//    }
//
//    private SequenceReader createNextSequenceReader()
//    {
//
//    }
//}
