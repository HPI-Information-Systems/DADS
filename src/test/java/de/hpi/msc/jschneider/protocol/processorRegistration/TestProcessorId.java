package de.hpi.msc.jschneider.protocol.processorRegistration;

import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestProcessorId extends TestCase
{
    public void testLocalProcessorIdEqualsRemoteProcessorId()
    {
        val localRootPath = "akka://MasterActorSystem-127_0_0_1-7788";
        val remoteRootPath = "akka://MasterActorSystem-127_0_0_1-7788@127.0.0.1:7788";

        assertThat(ProcessorId.of(localRootPath)).isEqualTo(ProcessorId.of(remoteRootPath));
    }

    public void testPathDepthDoesNotMatter()
    {
        val firstPath = "akka://MasterActorSystem-127_0_0_1-7788/user/FirstActor/SecondActor";
        val secondPath = "akka://MasterActorSystem-127_0_0_1-7788/";

        assertThat(ProcessorId.of(firstPath)).isEqualTo(ProcessorId.of(secondPath));
    }
}
