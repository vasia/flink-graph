package flink.gelly.school;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class LocalClusteringCoefficientITCase extends MultipleProgramsTestBase {

	public LocalClusteringCoefficientITCase(TestExecutionMode mode){
		super(mode);
	}

	private String edgesPath;
	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testSimpleGraph() throws Exception {
		/*
		 * Test Local Clustering Coefficient on the default data (directed graph)
		 */

		final String testEdges = "1	2\n" +
				"1	3\n" +
				"1	4\n" +
				"2	3\n" +
				"2	4\n" +
				"3	4\n" ;

		
		edgesPath = createTempFile(testEdges);

		LocalClusteringCoefficient.main(new String[] {edgesPath, resultPath});
		
		expectedResult = "1,0.5\n" +
				"2,0.5\n" +
				"3,0.5\n" +
				"4,0.5\n";
	}



	@Test
	public void testGraph() throws Exception {
		/*
		 * Test with a bidirectional edge and zero coefficient
		 */

		final String testEdges = "1	2\n" +
				"1	4\n" +
				"1	5\n" +
				"2	3\n" +
				"2	5\n" +
				"3	4\n" +
				"4	3\n" +
				"4	5\n" ;

		
		edgesPath = createTempFile(testEdges);

		LocalClusteringCoefficient.main(new String[] {edgesPath, resultPath});
		
		expectedResult = "1,0.3333333333333333\n" +
				"2,0.16666666666666666\n" +
				"3,0.0\n" +
				"4,0.16666666666666666\n"+
				"5,0.3333333333333333\n";
	}

	// -------------------------------------------------------------------------
	//  Util methods
	// -------------------------------------------------------------------------

	private String createTempFile(final String rows) throws Exception {
		File tempFile = tempFolder.newFile();
		Files.write(rows, tempFile, Charsets.UTF_8);
		return tempFile.toURI().toString();
	}
}
