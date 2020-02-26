package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class IdGenerator
{
    private static final AtomicInteger SUB_ID = new AtomicInteger();

    private static boolean initialized = false;
    private static int processorIdHash;

    public static void initialize(ProcessorId localProcessorId)
    {
        assert !initialized : "IdGenerator has already been initialized!";

        initialized = true;
        processorIdHash = localProcessorId.hashCode();
    }

    public static long next()
    {
        assert initialized : "IdGenerator has not been initialized!";
        return ByteBuffer.allocate(Long.BYTES).putInt(processorIdHash).putInt(SUB_ID.incrementAndGet()).getLong(0);
    }
}
