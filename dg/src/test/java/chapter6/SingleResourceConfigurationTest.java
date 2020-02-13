package chapter6;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SingleResourceConfigurationTest {
    @Test
    public void get() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("config/configuration-1.xml");
        assertThat(conf.get("color"), is("yellow"));
        assertThat(conf.getInt("size", 0), is(10));
        assertThat(conf.get("breath", "wide"), is("wide"));
    }
}
