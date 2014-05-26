package edu.zju.bme.clever.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.openehr.am.parser.ContentObject;
import org.openehr.am.parser.DADLParser;
import org.openehr.rm.binding.DADLBinding;
import org.openehr.rm.composition.content.entry.Observation;
import org.openehr.rm.support.identification.HierObjectID;

public class CleverServicePerformanceTest extends CleverServiceTestBase {

	public CleverServicePerformanceTest() throws IOException {
		super();
	}

	@Test
	public void testInsert1000() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		long start = System.currentTimeMillis();
		List<String> dadls = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			if (i % 100 == 0) {
				System.out.println(i);				
			}
			
			for (String dadl : getDadlFiles()) {
				File file = new File(dadl);
				InputStream is = new FileInputStream(file);
				DADLParser parser = new DADLParser(is);
				ContentObject contentObj = parser.parse();
				DADLBinding binding = new DADLBinding();
				Observation bp = (Observation) binding.bind(contentObj);
				UUID uuid = UUID.randomUUID();
				HierObjectID uid = new HierObjectID(uuid.toString());
				bp.setUid(uid);
				dadls.add(binding.toDADLString(bp));
			}			
		}

		cleverImpl.insert(dadls);

		long end = System.currentTimeMillis();
		System.out.println(end - start);

		cleanTestBaseData();
	}

	@Test
	public void testSelect1000() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		long start = System.currentTimeMillis();
		
		for (int i = 0; i < 1000; i++) {
			String query = "select o "
					+ "from openEHR-EHR-OBSERVATION.blood_pressure.v1 as o ";
			cleverImpl.select(query);
		}
		
		long end = System.currentTimeMillis();
		System.out.println(end - start);

		cleanTestBaseData();
	}

}
