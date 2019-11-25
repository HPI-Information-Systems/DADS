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
        val file = new File(path);
        return BinarySequenceReader.fromFile(file);
    }

    public void testSize()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.getSize()).isEqualTo(4);
        assertThat(reader.hasNext()).isTrue();
    }

    public void testReadSimpleSequence()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.next()).isEqualTo(-1.1f);
        assertThat(reader.next()).isEqualTo(0.9f);
        assertThat(reader.next()).isEqualTo(1.0f);
        assertThat(reader.next()).isEqualTo(4.25f);
        assertThat(reader.hasNext()).isFalse();
    }
}
