package de.hpi.msc.jschneider.fileHandling;

import junit.framework.TestCase;
import lombok.val;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinarySequenceReader extends TestCase
{
    private SequenceReader readerFromSimpleSequence()
    {
        val path = getClass().getClassLoader().getResource("simple-sequence.bin").getFile();
        return BinarySequenceReader.fromFile(new File(path));
    }

    public void testSize()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.getSize()).isEqualTo(4);
    }

    public void testReadSimpleSequence()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.next()).isEqualTo(-1.1f);
        assertThat(reader.next()).isEqualTo(0.9f);
        assertThat(reader.next()).isEqualTo(1.0f);
        assertThat(reader.next()).isEqualTo(4.25f);
    }
}
