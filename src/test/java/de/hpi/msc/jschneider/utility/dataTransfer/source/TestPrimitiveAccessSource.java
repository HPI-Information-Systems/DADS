package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveAccessSource extends TestCase
{
    public void testRead()
    {
        val matrix = (new MatrixInitializer(3L).appendRow(new float[]{0.0f, 4.0f, 8.0f})
                                               .appendRow(new float[]{1.0f, 5.0f, 9.0f})
                                               .appendRow(new float[]{2.0f, 6.0f, 10.0f})
                                               .appendRow(new float[]{3.0f, 7.0f, 11.0f})
                                               .create());
        val source = new PrimitiveAccessSource(matrix);

        assertThat(source.isAtEnd()).isFalse();

        assertThat(source.read(5L)).containsExactly(0.0f, 1.0f, 2.0f, 3.0f, 4.0f);
        assertThat(source.read(5L)).containsExactly(5.0f, 6.0f, 7.0f, 8.0f, 9.0f);
        assertThat(source.read(1L)).containsExactly(10.0f);
        assertThat(source.read(5L)).containsExactly(11.0f);
        assertThat(source.read(5L)).isEmpty();

        assertThat(source.isAtEnd()).isTrue();
    }
}
