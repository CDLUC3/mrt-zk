package org.cdlib.mrt.zk;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedHashMap;

import org.json.JSONObject;

public class StateTest {


    public StateTest() {
    }

    @Test
    public void validateEnumAgainstYml() throws FileNotFoundException {
      Yaml yaml = new Yaml();
      final Object loadedYaml = yaml.load(new FileReader(new File("states.yml")));
      //Note that jackson yaml parsing does not support anchors and references, so Gson is used instead.
      //serializeNulls makes yaml to json handling similar to ruby
      Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
      String json = gson.toJson(loadedYaml,LinkedHashMap.class);
      JSONObject jy = new JSONObject(json);

      assertTrue(IngestState.statesAsJson(JobState.values()).similar(jy.getJSONObject("job_states")));
      assertTrue(IngestState.statesAsJson(BatchState.values()).similar(jy.getJSONObject("batch_states")));
      assertTrue(IngestState.statesAsJson(AccessState.values()).similar(jy.getJSONObject("access_states")));
    }

    @Test
    public void InitialStatesTest() {
      BatchState state = BatchState.values()[0];
      assertEquals(state, BatchState.Pending);
      JobState jstate = JobState.values()[0];
      assertEquals(jstate, JobState.Pending);
      AccessState astate = AccessState.values()[0];
      assertEquals(astate, AccessState.Pending);
    }

    @Test
    public void BatchStatePendingToProcessing() {
      IngestState state = BatchState.values()[0];
      assertFalse(state.stateChangeAllowed(BatchState.Reporting));
      assertTrue(state.stateChangeAllowed(BatchState.Held));
      assertTrue(state.stateChangeAllowed(BatchState.Processing));
      state = state.stateChange(BatchState.Processing);
      assertNotNull(state);
      assertEquals(state, BatchState.Processing);
    }

    @Test
    public void BatchStatePendingHeldProcessing() {
      IngestState state = BatchState.values()[0];
      assertNull(state.success());
      state = state.stateChange(BatchState.Held);
      assertNotNull(state);
      assertEquals(state, BatchState.Held);

      state = state.stateChange(BatchState.Pending);
      assertNotNull(state);
      assertEquals(state, BatchState.Pending);

      state = state.stateChange(BatchState.Processing);
      assertNotNull(state);
      assertEquals(state, BatchState.Processing);      
    }

    @Test
    public void BatchStatePendingToEstimating() {
      IngestState state = JobState.values()[0];
      assertFalse(state.stateChangeAllowed(JobState.Processing));
      assertTrue(state.stateChangeAllowed(JobState.Held));
      assertTrue(state.stateChangeAllowed(JobState.Estimating));
      state = state.stateChange(JobState.Estimating);
      assertNotNull(state);
      assertEquals(state, JobState.Estimating);
    }

    @Test
    public void JobPendingHeldEstimating() {
      IngestState state = JobState.values()[0];
      assertNull(state.success());
      state = state.stateChange(JobState.Held);
      assertNotNull(state);
      assertEquals(state, JobState.Held);

      state = state.stateChange(JobState.Pending);
      assertNotNull(state);
      assertEquals(state, JobState.Pending);

      state = state.stateChange(JobState.Estimating);
      assertNotNull(state);
      assertEquals(state, JobState.Estimating);      
    }

    @Test
    public void BatchHappyPath() {
      IngestState state = BatchState.values()[0];
      state = state.stateChange(BatchState.Processing);
      state = state.success();
      assertNotNull(state);
      assertEquals(state, BatchState.Reporting);

      state = state.success();
      assertNotNull(state);
      assertEquals(state, BatchState.Completed);

      assertTrue(state.isDeletable());
    }

    @Test 
    public void JobHappyPath() {
      IngestState state = JobState.values()[0];
      assertNull(state.success());

      state = state.stateChange(JobState.Estimating);
      assertNotNull(state);

      assertEquals(state, JobState.Estimating);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Provisioning);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Downloading);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Processing);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Recording);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Notify);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Completed);      
      assertTrue(state.isDeletable());
    }

    @Test
    public void BatchFailPath() {
      IngestState state = BatchState.values()[0];
      state = state.stateChange(BatchState.Processing);
      state = state.success();
      assertNotNull(state);
      assertEquals(state, BatchState.Reporting);

      state = state.fail();
      assertNotNull(state);
      assertEquals(state, BatchState.Failed);

      state = state.stateChange(BatchState.Deleted);
      assertEquals(state, BatchState.Deleted);
      assertTrue(state.isDeletable());
    }

    @Test 
    public void JobFailPath() {
      IngestState state = JobState.values()[0];
      assertNull(state.success());

      state = state.stateChange(JobState.Estimating);
      assertNull(state.fail());
      assertNotNull(state);

      assertEquals(state, JobState.Estimating);      
      assertNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Provisioning);      
      assertNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Downloading);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Processing);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Recording);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Notify);      
      state = state.fail();
      assertNotNull(state);

      assertEquals(state, JobState.Failed);
      state = state.stateChange(JobState.Deleted);
      assertTrue(state.isDeletable());
    }

    @Test
    public void BatchRecoveryPath() {
      IngestState state = BatchState.values()[0];
      state = state.stateChange(BatchState.Processing);
      state = state.success();
      assertNotNull(state);
      assertEquals(state, BatchState.Reporting);

      state = state.fail();
      assertNotNull(state);
      assertEquals(state, BatchState.Failed);

      state = state.stateChange(BatchState.UpdateReporting);      
      assertEquals(state, BatchState.UpdateReporting);

      state = state.success();
      assertNotNull(state);
      assertEquals(state, BatchState.Completed);

      assertTrue(state.isDeletable());
    }

    @Test 
    public void JobRecoveryPath() {
      IngestState state = JobState.values()[0];
      assertNull(state.success());

      state = state.stateChange(JobState.Estimating);
      assertNull(state.fail());
      assertNotNull(state);

      assertEquals(state, JobState.Estimating);      
      assertNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Provisioning);      
      assertNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Downloading);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Processing);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Recording);      
      assertNotNull(state.fail());
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Notify);      
      state = state.fail();
      assertNotNull(state);

      assertEquals(state, JobState.Failed);
      state = state.stateChange(JobState.Notify);
      assertNotNull(state);

      assertEquals(state, JobState.Notify);      
      state = state.success();
      assertNotNull(state);

      assertEquals(state, JobState.Completed);      
      assertTrue(state.isDeletable());
    }

}