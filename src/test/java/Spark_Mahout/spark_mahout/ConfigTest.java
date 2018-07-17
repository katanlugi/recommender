package Spark_Mahout.spark_mahout;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

public class ConfigTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseArgs() {
		String[] args = {
			"-dbs", "https://ti.bfh.ch",
			"-dbn", "false",
			"-dbu", "false",
			"-dbp", "false",
			"-src", "somepath",
			"-ft", "false",
			"-e", "false",
			"-m", "10",		// number of iterations
			"-i", "6", // number of recommendations
			"-nf", "1",
			"-im", "false",
			"-nn", "false",
			"-s", "false",
			"-v"
		};
		Config conf = Config.parseArgs(args);
		Assert.notNull(conf, "conf object should not be null");
	}
	
	@Test
	public void testParseBolleanString() {
		Assert.isTrue(Utils.parseBooleanString("yes"), "yes should be true");
		Assert.isTrue(Utils.parseBooleanString("YES"), "YES should be true");
		Assert.isTrue(Utils.parseBooleanString("true"), "true should be true");
		Assert.isTrue(Utils.parseBooleanString("1"), "1 should be true");
		
		Assert.isTrue(!Utils.parseBooleanString("no"), "no should be false");
		Assert.isTrue(!Utils.parseBooleanString("No"), "No should be false");
		Assert.isTrue(!Utils.parseBooleanString("false"), "false should be false");
		Assert.isTrue(!Utils.parseBooleanString("0"), "0 should be false");
		Assert.isTrue(!Utils.parseBooleanString("asdf"), "asdf should be false");
	}
	
	@Test
	public void testInvalidNumberValues() {
		String[] m = {"-m", "not_a_number"};
		String[] i = {"-i", "not_a_number"};
		String[] nf = {"-nf", "not_a_number"};
		Assert.isNull(Config.parseArgs(m), "Config should be null with invalid -m");
		Assert.isNull(Config.parseArgs(i), "Config should be null with invalid -i");
		Assert.isNull(Config.parseArgs(nf), "Config should be null with invalid -nf");
	}
	
	@Test
	public void testUsage() {
		Config.usage();
	}

	@Test
	public void testToString() {
		Config c = new Config();
		Assert.notNull(c.toString(), "To string returned null but should not be null");
	}
}
