package org.cdlib.mrt.zk;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import org.json.JSONException;
import org.json.JSONObject;

public class ZKTest {
    enum Tests {
      test_read_write_from_zk,
      test_sequential_node_mapping,
      ephemeral_node_remaining,
      ephemeral_node_destroyed,
      create_batch,
      create_and_load_batch,
      load_non_existent_batch,
      batch_with_lock_unlock,
      batch_with_ephemeral_released_lock,
      batch_with_unreleased_lock,
      modify_batch_state,
      batch_acquire,
      create_job,
      create_job_state_change,
      load_job_state_change,
      acquire_pending_job,
      acquire_lowest_priority_job,
      job_happy_path,
      batch_happy_path,
      batch_failure,
      batch_recovery,
      job_happy_path_with_delete,
      batch_happy_path_with_delete;
    }

    private ZooKeeper zk;
    private JSONObject testCasesJson;
    private Tests currentTest;
    private Map<String, String> remap = new HashMap<>();

    public ZKTest() throws IOException {
      Yaml yaml = new Yaml();
      final Object testCasesYaml = yaml.load(new FileReader(new File("test-cases.yml")));
      //Note that jackson yaml parsing does not support anchors and references, so Gson is used instead.
      //serializeNulls makes yaml to json handling similar to ruby
      Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
      String json = gson.toJson(testCasesYaml,LinkedHashMap.class);
      testCasesJson = new JSONObject(json);

      zk = createZk();
    }

    public static ZooKeeper createZk() throws IOException {
      return new ZooKeeper("localhost:8084", 100, null);
    }

    public String makePath(String path, String cp) {
      if (path.equals("/")) {
        return String.format("/%s", cp);
      }
      return String.format("%s/%s", path, cp);
    }

    public void create(String s, Object data) throws KeeperException, InterruptedException {
      create(Paths.get(s), data);
    }

    public void create(Path path, Object data) throws KeeperException, InterruptedException {
      Path par = path.getParent();
      if (!QueueItemHelper.exists(zk, par.toString())) {
        create(par, null);
      }
      QueueItemHelper.create(zk, path.toString(), QueueItemHelper.serializeAsBytes(data));
    }

    public void initPaths() throws KeeperException, InterruptedException {
      create("/jobs/states", null);
      create("/batches", null);
    }

    public void clearPaths(String root) throws KeeperException, InterruptedException {
      for(String s: zk.getChildren(root, false)) {
        String p = makePath(root, s);
        if (p.equals("/zookeeper")) {
          continue;
        }
        clearPaths(p);
        //System.out.println("DEL "+p);
        zk.delete(p, -1);
      }
    }

    @Before
    public void initTest() throws KeeperException, InterruptedException {
      clearPaths("/");
      initPaths();
      remap.clear();
      remap.put("now", "\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d .\\d\\d\\d\\d");
    }

    public boolean verifyOutput(Tests test) throws KeeperException, InterruptedException {
      String lists = list(new JSONObject(), "/").toString();
      for(String k: remap.keySet()) {
        lists = lists.replaceAll(remap.get(k), k);
      }

      JSONObject curzk = new JSONObject(lists);
      //System.out.println(curzk.toString(2));
      JSONObject jout = getInOut(test, "output");
      //System.out.println(jout.toString(2));

      if (curzk.similar(jout)) {
        return true;
      }

      ArrayList<String> diffs = new ArrayList<>();
      for(String s: curzk.keySet()) {
        if (!jout.has(s)) {
          diffs.add(s);
        }
      }
      if (!diffs.isEmpty()) {
        System.out.println("Keys only in ZK: " + diffs);
      }
      diffs.clear();

      for(String s: jout.keySet()) {
        if (!curzk.has(s)) {
          diffs.add(s);
        }
      }
      if (!diffs.isEmpty()) {
        System.out.println("Keys only in output: " + diffs);
      }

      for(String s: curzk.keySet()) {
        Object cur = curzk.get(s);
        Object j = jout.get(s);
        if (cur.equals(j)) {
          continue;
        }
        if (cur instanceof JSONObject && j instanceof JSONObject) {
          if (((JSONObject)cur).similar((JSONObject)j)) {
            continue;
          }
        }
        System.out.println(String.format("Key Diff %s: %s|%s", s, curzk.get(s), jout.get(s)));
      }

      return false;
    }

    @After
    public void reviewTest() throws KeeperException, InterruptedException {
      assertTrue(verifyOutput(currentTest));
    }

    public JSONObject getInOut(Tests testcase, String inout) {
      JSONObject config = testCasesJson.getJSONObject(testcase.name());
      if (config != null) {
        Object temp = config.get(inout);
        if (temp instanceof JSONObject) {
          return config.getJSONObject(inout);
        }
      }
      return new JSONObject();
    }


    public void load(Tests testcase) throws KeeperException, InterruptedException {
      this.currentTest = testcase;
      JSONObject configin = getInOut(currentTest, "input");
      for(String k: configin.keySet()) {
        create(k, configin.get(k));
      }  
    }

    public boolean skipListing(String path, Object dp) throws KeeperException, InterruptedException {
      if (path.equals("/") || path.equals("/batches") || path.equals("/jobs") || path.equals("/jobs/states")) {
        return true;
      }
      if (zk.getChildren(path, false).isEmpty()) {
        if (Paths.get(path).getParent().toString().equals("/jobs/states")) {
          return true;
        }
        return false;
      }
      if (dp == null) {
        return true;
      }
      if (dp.toString().isEmpty() || dp == JSONObject.NULL) {
        return true;
      }
      return false;
    }

    public JSONObject list(JSONObject obj, String path) throws KeeperException, InterruptedException {
      if (path.equals("/zookeeper")) {
        return obj;
      }
      String data = QueueItemHelper.pathToString(zk, path);
      Object dp = data;
      if (data == null) {
      } else if (data.equals("null") || data.isEmpty()) {
        dp = JSONObject.NULL;
      } else {
        try {
          dp = new JSONObject(data);
        } catch(JSONException e) {
          try {
            dp = Integer.parseInt(data);
          } catch(NumberFormatException e2) {
            try {
              dp = Long.parseLong(data);
            } catch(NumberFormatException e3) {
              //no action
            }
          }
          //no action
        }  
      }
      if (!skipListing(path, dp)) {
        obj.put(path, dp);
      }
      for(String cp: zk.getChildren(path, false)) {
        list(obj, makePath(path, cp));
      }
      return obj;
    }

    @Test
    public void readWriteFromZk() throws FileNotFoundException, KeeperException, InterruptedException {
      load(Tests.test_read_write_from_zk);
    }

    @Test
    public void sequentialNodeMapping() throws FileNotFoundException, KeeperException, InterruptedException {
      load(Tests.test_sequential_node_mapping);
      remap.put("/foo0", QueueItemHelper.createSequential(zk, "/foo", QueueItemHelper.empty));
      remap.put("/foo1", QueueItemHelper.createSequential(zk, "/foo", QueueItemHelper.empty));
      remap.put("/foo2", QueueItemHelper.createSequential(zk, "/foo", QueueItemHelper.empty));
    }

    @Test
    public void createEphemeralNode() throws FileNotFoundException, KeeperException, InterruptedException {
      load(Tests.ephemeral_node_remaining);
      QueueItemHelper.createEphemeral(zk, "/foo", QueueItemHelper.empty);
    }

    @Test
    public void createEphemeralNodeAndTerminateClient() throws KeeperException, InterruptedException, IOException {
      load(Tests.ephemeral_node_destroyed);
      //Auto-close the ZK client
      try(ZooKeeper zktemp = createZk()){
        QueueItemHelper.createEphemeral(zktemp, "/foo", QueueItemHelper.empty);
      }
    }

    public JSONObject fooBar() {
      return fooBar("");
    }

    public JSONObject fooBar(String suffix) {
      JSONObject json = new JSONObject();
      json.put("foo", "bar" + suffix);
      return json;
    }

    public JSONObject quack() {
      return quack("");
    }

    public JSONObject quack(String suffix) {
      JSONObject json = new JSONObject();
      json.put("job", "quack"+suffix);
      return json;
    }

    @Test
    public void createBatch() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.create_batch);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
    }

    @Test
    public void createAndLoadBatch() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.create_batch);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch b2 = new Batch(b.id());
      b2.load(zk);
      assertEquals(b2.status(), BatchState.Pending);
    }

    @Test
    public void loadNonExistentBatch() throws KeeperException, InterruptedException {
      load(Tests.load_non_existent_batch);
      Batch b = new Batch("111");
      
      assertThrows(MerrittZKNodeInvalid.class, () ->
        b.load(zk)
      );
    }

    @Test
    public void batchLockWithUnlock() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_with_lock_unlock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.lock(zk);
      b.unlock(zk);
    }

    @Test
    public void batchLockWithEphemeralReleasedLock() throws KeeperException, InterruptedException, IOException, MerrittZKNodeInvalid{
      load(Tests.batch_with_ephemeral_released_lock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      try(ZooKeeper zktemp = createZk()) {
        b.lock(zktemp);
      }
    }

    @Test
    public void batchLockWithUnreleasedLock() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_with_unreleased_lock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.lock(zk);
    }

    @Test
    public void batchAcquire() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_acquire);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      Batch b2 = Batch.createBatch(zk, fooBar("2"));
      remap.put("bid1", b2.id());
      assertNotNull(Batch.acquirePendingBatch(zk));
      assertNotNull(Batch.acquirePendingBatch(zk));
      assertNull(Batch.acquirePendingBatch(zk));
    }

    @Test
    public void modifyBatchState() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.modify_batch_state);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.setStatus(zk, b.status().stateChange(BatchState.Processing));
    }

    @Test
    public void createJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.create_job);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack());
      remap.put("jid0", j.id());

      assertEquals(j.bid(), bb.id());
    }

    @Test
    public void createJobStateChange() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.create_job_state_change);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack());
      remap.put("jid0", j.id());

      j.setStatus(zk, j.status().stateChange(JobState.Estimating));
    }

    @Test
    public void createAndLoadJobStateChange() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.load_job_state_change);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack());
      remap.put("jid0", j.id());
      assertEquals(j.bid(), bb.id());

      Job jj = new Job(j.id());
      jj.load(zk);
      assertEquals(jj.bid(), bb.id());
      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
    }

    @Test
    public void acquirePendingJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.acquire_pending_job);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack());
      remap.put("jid0", j.id());
      assertEquals(j.bid(), bb.id());

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
    }

    @Test
    public void acquireLowestPriorityJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.acquire_lowest_priority_job);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("1"));
      remap.put("jid0", j.id());

      j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      j = Job.createJob(zk, bb.id(), quack("3"));
      remap.put("jid2", j.id());

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
    }

    @Test
    public void jobHappyPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.job_happy_path);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("1"));
      remap.put("jid0", j.id());

      j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      j = Job.createJob(zk, bb.id(), quack("3"));
      remap.put("jid2", j.id());

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      assertFalse(jj.status().isDeletable());

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);
      assertTrue(jj.status().isDeletable());
    }

    @Test
    public void batchHappyPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_happy_path);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      Batch bbb = Batch.acquireCompletedBatch(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);

      bbb = Batch.acquireCompletedBatch(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertFalse(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().success());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Completed);
      assertTrue(bbb.status().isDeletable());

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Completed);
      assertFalse(bbbb.hasFailure());
    }

    @Test
    public void batchFailure() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_failure);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      Batch bbb = Batch.acquireCompletedBatch(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().fail());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Failed);

      bbb = Batch.acquireCompletedBatch(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertTrue(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().fail());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Failed);
      assertFalse(bbb.status().isDeletable());

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Failed);
      assertTrue(bbbb.hasFailure());

      bbbb.setStatus(zk, BatchState.Deleted);
      assertEquals(bbbb.status(), BatchState.Deleted);
      assertTrue(bbbb.status().isDeletable());
    }

    @Test
    public void batchRecovery() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.batch_recovery);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      Batch bbb = Batch.acquireCompletedBatch(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().fail());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Failed);

      bbb = Batch.acquireCompletedBatch(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertTrue(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().fail());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Failed);
      assertFalse(bbb.status().isDeletable());

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Failed);

      Job jjj = new Job(jj.id());
      jjj.load(zk);
      jjj.setStatusWithRetry(zk, JobState.Notify);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);

      assertEquals(jj.status(), JobState.Completed);

      bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Failed);
      assertFalse(bbbb.hasFailure());

      bbbb.setStatus(zk, BatchState.UpdateReporting);
      assertFalse(bbbb.status().isDeletable());

      bbbb.setStatus(zk, bbbb.status().success());
      bbbb.setStatus(zk, BatchState.Completed);
      assertTrue(bbbb.status().isDeletable());
    }

    @Test
    public void jobHappyPathWithDelete() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.job_happy_path_with_delete);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("1"));
      remap.put("jid0", j.id());

      j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      j = Job.createJob(zk, bb.id(), quack("3"));
      remap.put("jid2", j.id());

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      assertFalse(jj.status().isDeletable());

      final Job fj = jj;

      assertThrows(MerrittStateError.class, () ->
        fj.delete(zk)
      );


      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);
      assertTrue(jj.status().isDeletable());

      jj.delete(zk);
    }

    @Test
    public void batchHappyPathWithDelete() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_happy_path_with_delete);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setPriority(zk, 2);

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
      jj.unlock(zk);

      jj = Job.acquireJob(zk, JobState.Estimating);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Provisioning);

      jj = Job.acquireJob(zk, JobState.Provisioning);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Downloading);

      jj = Job.acquireJob(zk, JobState.Downloading);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Processing);

      jj = Job.acquireJob(zk, JobState.Processing);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Notify);
      Batch bbb = Batch.acquireCompletedBatch(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);

      bbb = Batch.acquireCompletedBatch(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertFalse(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().success());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Completed);
      assertTrue(bbb.status().isDeletable());

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Completed);
      assertFalse(bbbb.hasFailure());

      bbbb.delete(zk);
    }

}