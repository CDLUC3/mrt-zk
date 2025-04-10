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
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.json.JSONException;
import org.json.JSONObject;

public class ZKTestIT {
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
      batch_happy_path_with_delete,
      batch_happy_path_with_delete_completed,
      job_create_config,
      job_create_config_ident,
      job_create_config_ident_metadata,
      lock_ingest,
      lock_access,
      lock_collection,
      lock_store,
      lock_inventory,
      access_happy_path,
      access_happy_path_del,
      access_fail_path,
      find_batch_by_uuid;
    }

    private ZooKeeper zk;
    private JSONObject testCasesJson;
    private Tests currentTest;
    private Map<String, String> remap = new HashMap<>();
    private static int port = 8084;
    private static String host = "localhost";

    public ZKTestIT() throws IOException {
      Yaml yaml = new Yaml();
      final Object testCasesYaml = yaml.load(new FileReader(new File("test-cases.yml")));
      //Note that jackson yaml parsing does not support anchors and references, so Gson is used instead.
      //serializeNulls makes yaml to json handling similar to ruby
      Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
      String json = gson.toJson(testCasesYaml,LinkedHashMap.class);
      testCasesJson = new JSONObject(json);

      try {
        port = Integer.parseInt(System.getenv("zoo.port"));
      } catch (NumberFormatException e) {
        System.err.println("zoo.port not set... using default value");
      }

      zk = createZk();
    }

    public static ZooKeeper createZk() throws IOException {
      ZooKeeper zk = new ZooKeeper(String.format("%s:%d", host, port), 5000, null);
      for(int i=0; i < 100; i++) {
        if (zk.getState() == States.CONNECTED) {
          return zk;
        }
        try {
          Thread.sleep(400);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return zk;
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
      Job.initNodes(zk);
      Access.initNodes(zk);
      MerrittLocks.initLocks(zk);
    }

    public void clearPaths(String root) throws KeeperException, InterruptedException, IOException {
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
    public void initTest() throws KeeperException, InterruptedException, IOException {
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
        Object cur = "";
        try {
          cur = curzk.get(s);
        } catch(JSONException e) {
          //no action
        }
        Object j = jout.has(s) ? jout.get(s) : "";
        if (cur.equals(j)) {
          continue;
        }
        if (cur instanceof JSONObject && j instanceof JSONObject) {
          if (((JSONObject)cur).similar((JSONObject)j)) {
            continue;
          }
        }
        System.out.println(String.format("Key Diff %s: %s|%s", s, cur, j));
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
      if (path.equals("/") || 
          path.equals(QueueItem.ZkPaths.Batch.path) || 
          path.equals(QueueItem.ZkPaths.BatchUuids.path) || 
          path.equals(QueueItem.ZkPaths.Job.path) || 
          path.equals(QueueItem.ZkPaths.JobStates.path)) {
        return true;
      }
      if (path.equals(QueueItem.ZkPaths.LocksInventory.path) || 
          path.equals(QueueItem.ZkPaths.LocksCollections.path) || 
          path.equals(QueueItem.ZkPaths.LocksLocalID.path) || 
          path.equals(QueueItem.ZkPaths.LocksStorage.path) || 
          path.equals(QueueItem.ZkPaths.LocksQueue.path)) {
        return true;
      }
      if (path.equals(QueueItem.ZkPaths.Access.path) || 
          path.equals(QueueItem.ZkPaths.AccessSmall.path) || 
          path.equals(QueueItem.ZkPaths.AccessLarge.path)) {
        return true;
      }
      if (zk.getChildren(path, false).isEmpty()) {
        if (Paths.get(path).getParent().toString().equals(QueueItem.ZkPaths.JobStates.path)) {
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
      return fooBar(suffix, "bid-uuid");
    }

    public JSONObject fooBar(String suffix, String uuid) {
      JSONObject json = new JSONObject();
      json.put("foo", "bar" + suffix);
      json.put(MerrittJsonKey.BatchId.key(), uuid);
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
    public void createBatch() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.create_batch);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
    }

    @Test
    public void createAndLoadBatch() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
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
    public void batchLockWithUnlock() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_with_lock_unlock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.lock(zk);
      b.unlock(zk);
    }

    @Test
    public void batchLockWithEphemeralReleasedLock() throws KeeperException, InterruptedException, IOException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_with_ephemeral_released_lock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      try(ZooKeeper zktemp = createZk()) {
        b.lock(zktemp);
      }
    }

    @Test
    public void batchLockWithUnreleasedLock() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_with_unreleased_lock);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.lock(zk);
    }

    @Test
    public void batchAcquire() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_acquire);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      Batch b2 = Batch.createBatch(zk, fooBar("2", "bid-uuid2"));
      remap.put("bid1", b2.id());
      Batch bb = Batch.acquirePendingBatch(zk);
      assertNotNull(bb);
      assertEquals(bb.status(), BatchState.Processing);
      bb = Batch.acquirePendingBatch(zk);
      assertNotNull(bb);
      bb = Batch.acquirePendingBatch(zk);
      assertNull(bb);
    }

    @Test
    public void modifyBatchState() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
      load(Tests.modify_batch_state);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      b.setStatus(zk, b.status().stateChange(BatchState.Processing));
    }

    @Test
    public void createJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
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
    public void createJobStateChange() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
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
    public void createAndLoadJobStateChange() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
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
    public void acquirePendingJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
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
      assertNotNull(jj);
      Job jj2 = Job.acquireJob(zk, JobState.Pending);
      assertNull(jj2);
      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
    }

    @Test
    public void acquireLowestPriorityJob() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.acquire_lowest_priority_job);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("1"));
      remap.put("jid0", j.id());

      j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

      j = Job.createJob(zk, bb.id(), quack("3"));
      remap.put("jid2", j.id());

      bb.unlock(zk);

      Job jj = Job.acquireJob(zk, JobState.Pending);
      assertEquals(jj.id(), remap.get("jid1"));

      jj.setStatus(zk, jj.status().stateChange(JobState.Estimating));
    }

    @Test
    public void jobHappyPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.job_happy_path);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("1"));
      remap.put("jid0", j.id());

      j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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

      jj.setInventory(zk, "http://storage.manifest.url", "tbd");
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Recording);

      jj = Job.acquireJob(zk, JobState.Recording);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      assertEquals(jj.inventoryManifestUrl(), "http://storage.manifest.url");
      assertEquals(jj.inventoryMode(), "tbd");

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

      List<Job> jobs = bb.getProcessingJobs(zk);
      assertEquals(jobs.size(), 2);
      jobs = bb.getCompletedJobs(zk);
      assertEquals(jobs.size(), 1);
      jobs = bb.getFailedJobs(zk);
      assertEquals(jobs.size(), 0);

      jobs = Job.listJobs(zk, null);
      assertEquals(jobs.size(), 3);
      jobs = Job.listJobs(zk, JobState.Pending);
      assertEquals(jobs.size(), 2);
    }

    @Test
    public void batchHappyPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
      load(Tests.batch_happy_path);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      Batch bbb = Batch.acquireBatchForReporting(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);

      bbb = Batch.acquireBatchForReporting(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertFalse(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().success());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Completed);
      assertTrue(bbb.status().isDeletable());

      Batch bbbx = Batch.acquireBatchForReporting(zk);
      assertNull(bbbx);

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Completed);
      assertFalse(bbbb.hasFailure());
    }

    @Test
    public void batchFailure() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
      load(Tests.batch_failure);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      Batch bbb = Batch.acquireBatchForReporting(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().fail(), "Sample Failure Message");
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Failed);

      bbb = Batch.acquireBatchForReporting(zk);
      assertNotNull(bbb);
      assertEquals(bbb.status(), BatchState.Reporting);
      assertFalse(bbb.status().isDeletable());
      assertTrue(bbb.hasFailure());
      bbb.setStatus(zk, bbb.status().fail());
      bbb.unlock(zk);

      assertEquals(bbb.status(), BatchState.Failed);
      assertFalse(bbb.status().isDeletable());
      assertTrue(bbb.hasFailure());

      jj.setStatus(zk, JobState.Deleted);

      Batch bbbb = new Batch(bbb.id());
      bbbb.load(zk);
      assertEquals(bbbb.status(), BatchState.Failed);

      List<Job> jobs = bb.getProcessingJobs(zk);
      assertEquals(jobs.size(), 0);
      jobs = bb.getCompletedJobs(zk);
      assertEquals(jobs.size(), 0);
      jobs = bb.getFailedJobs(zk);
      assertEquals(jobs.size(), 0);
      jobs = bb.getDeletedJobs(zk);
      assertEquals(jobs.size(), 1);

      bbbb.setStatus(zk, BatchState.Deleted);
      assertEquals(bbbb.status(), BatchState.Deleted);
      assertTrue(bbbb.status().isDeletable());
    }

    @Test
    public void batchRecovery() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_recovery);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      Batch bbb = Batch.acquireBatchForReporting(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().fail());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Failed);

      bbb = Batch.acquireBatchForReporting(zk);
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
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      Batch bbb = Batch.acquireBatchForReporting(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);

      bbb = Batch.acquireBatchForReporting(zk);
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

    @Test
    public void batchHappyPathWithDeleteCompleted() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.batch_happy_path_with_delete_completed);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), quack("2"));
      remap.put("jid1", j.id());
      j.setStatusWithPriority(zk, JobState.Pending, 2);

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
      Batch bbb = Batch.acquireBatchForReporting(zk);
      assertNull(bbb);

      jj = Job.acquireJob(zk, JobState.Notify);
      assertNotNull(jj);
      assertEquals(jj.id(), remap.get("jid1"));
      jj.setStatus(zk, jj.status().success());
      jj.unlock(zk);
      assertEquals(jj.status(), JobState.Completed);

      bbb = Batch.acquireBatchForReporting(zk);
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

      List<String> ids = Batch.deleteCompletedBatches(zk);
      assertTrue(ids.contains(bbbb.id()));
    }

    @Test
    public void jobCreateConfig() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.job_create_config);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(zk, bb.id(), Job.createJobConfiguration("a", "b", "c", "d", "e"));
      remap.put("jid0", j.id());

      bb.unlock(zk);

      Job jj = new Job(remap.get("jid0"));
      jj.load(zk);

      assertEquals("a", jj.profileName());
      assertEquals("b", jj.submitter());
      assertEquals("c", jj.payloadUrl());
      assertEquals("d", jj.payloadType());
      assertEquals("e", jj.responseType());
    }

    @Test
    public void jobCreateConfigIdentifiers() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.job_create_config_ident);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(
        zk, 
        bb.id(), 
        Job.PRIORITY,
        Job.createJobConfiguration("a", "b", "c", "d", "e"),
        Job.createJobIdentifiers("f", "g;h")
      );
      remap.put("jid0", j.id());

      bb.unlock(zk);

      Job jj = new Job(remap.get("jid0"));
      jj.load(zk);

      assertEquals("f", jj.primaryId());
      assertEquals("g;h", jj.localId());
    }

    @Test
    public void jobCreateConfigIdentifiersMetadata() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.job_create_config_ident_metadata);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());

      Batch bb = Batch.acquirePendingBatch(zk);
      assertEquals(b.id(), bb.id());

      Job j = Job.createJob(
        zk, 
        bb.id(), 
        Job.PRIORITY,
        Job.createJobConfiguration("a", "b", "c", "d", "e"),
        Job.createJobIdentifiers("f", "g;h"),
        Job.createJobMetadata("i", "j", "k", "l")
      );
      remap.put("jid0", j.id());

      bb.unlock(zk);

      Job jj = new Job(remap.get("jid0"));
      jj.load(zk);

      assertEquals("i", jj.ercWho());
      assertEquals("j", jj.ercWhat());
      assertEquals("k", jj.ercWhen());
      assertEquals("l", jj.ercWhere());
    }

    @Test
    public void lockIngestQueue() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.lock_ingest);
      //ignore if lock does not exist
      MerrittLocks.unlockIngestQueue(zk);

      assertFalse(MerrittLocks.checkLockIngestQueue(zk));
      assertTrue(MerrittLocks.lockIngestQueue(zk));
      assertTrue(MerrittLocks.checkLockIngestQueue(zk));
      assertFalse(MerrittLocks.lockIngestQueue(zk));
      MerrittLocks.unlockIngestQueue(zk);
      assertTrue(MerrittLocks.lockIngestQueue(zk));
    }

    @Test
    public void lockAccessQueue() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.lock_access);
      assertFalse(MerrittLocks.checkLockLargeAccessQueue(zk));
      assertTrue(MerrittLocks.lockLargeAccessQueue(zk));
      assertTrue(MerrittLocks.checkLockLargeAccessQueue(zk));
      assertFalse(MerrittLocks.lockLargeAccessQueue(zk));
      MerrittLocks.unlockLargeAccessQueue(zk);
      assertTrue(MerrittLocks.lockLargeAccessQueue(zk));

      assertTrue(MerrittLocks.lockSmallAccessQueue(zk));
      assertFalse(MerrittLocks.lockSmallAccessQueue(zk));
      MerrittLocks.unlockSmallAccessQueue(zk);
      assertTrue(MerrittLocks.lockSmallAccessQueue(zk));
    }

    @Test
    public void lockCollection() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.lock_collection);
      assertFalse(MerrittLocks.checkLockCollection(zk, "foo"));
      assertTrue(MerrittLocks.lockCollection(zk, "foo"));
      assertTrue(MerrittLocks.checkLockCollection(zk, "foo"));
      assertFalse(MerrittLocks.lockCollection(zk, "foo"));
      MerrittLocks.unlockCollection(zk, "foo");
      assertTrue(MerrittLocks.lockCollection(zk, "foo"));

      assertTrue(MerrittLocks.lockCollection(zk, "bar"));
      assertFalse(MerrittLocks.lockCollection(zk, "bar"));
      MerrittLocks.unlockCollection(zk, "bar");
      assertTrue(MerrittLocks.lockCollection(zk, "bar"));
    }

    @Test
    public void lockStore() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.lock_store);
      assertFalse(MerrittLocks.checkLockObjectStorage(zk, "ark:/aaa/111"));
      assertTrue(MerrittLocks.lockObjectStorage(zk, "ark:/aaa/111"));
      assertTrue(MerrittLocks.checkLockObjectStorage(zk, "ark:/aaa/111"));
      assertFalse(MerrittLocks.lockObjectStorage(zk, "ark:/aaa/111"));
      MerrittLocks.unlockObjectStorage(zk, "ark:/aaa/111");
      assertTrue(MerrittLocks.lockObjectStorage(zk, "ark:/aaa/111"));

      assertTrue(MerrittLocks.lockObjectStorage(zk, "ark:/bbb/222"));
      assertFalse(MerrittLocks.lockObjectStorage(zk, "ark:/bbb/222"));
      MerrittLocks.unlockObjectStorage(zk, "ark:/bbb/222");
      assertTrue(MerrittLocks.lockObjectStorage(zk, "ark:/bbb/222"));
    }

    @Test
    public void lockInventory() throws KeeperException, InterruptedException, MerrittZKNodeInvalid{
      load(Tests.lock_inventory);
      assertTrue(MerrittLocks.lockObjectInventory(zk, "ark:/aaa/111"));
      assertFalse(MerrittLocks.lockObjectInventory(zk, "ark:/aaa/111"));
      MerrittLocks.unlockObjectInventory(zk, "ark:/aaa/111");
      assertTrue(MerrittLocks.lockObjectInventory(zk, "ark:/aaa/111"));

      assertTrue(MerrittLocks.lockObjectInventory(zk, "ark:/bbb/222"));
      assertFalse(MerrittLocks.lockObjectInventory(zk, "ark:/bbb/222"));
      MerrittLocks.unlockObjectInventory(zk, "ark:/bbb/222");
      assertTrue(MerrittLocks.lockObjectInventory(zk, "ark:/bbb/222"));
    }

    public JSONObject token(String val) {
      JSONObject json = new JSONObject();
      json.put("token", val);
      return json;
    }


    @Test
    public void accessHappyPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError, MerrittStateError{
      load(Tests.access_happy_path);
      Access a = Access.createAssembly(zk, Access.Queues.small, token("abc"));
      remap.put("qid0", a.id());
      Access aa = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNotNull(aa);
      assertEquals(a.id(), aa.id());
      assertEquals(aa.status(), AccessState.Pending);
      Access aa2 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa2);
      aa.setStatus(zk, AccessState.Processing);
      assertEquals(aa.status(), AccessState.Processing);
      aa.unlock(zk);

      Access aaa = new Access(Access.Queues.small, a.id());
      aaa.load(zk);
      assertEquals(a.id(), aaa.id());
      aaa.setStatus(zk, aaa.status().success());
      assertEquals(aaa.status(), AccessState.Completed);
      assertTrue(aaa.status().isDeletable());
      aaa.unlock(zk);
      Access aa3 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa3);
      List<Access> jobs = Access.listJobs(zk, Access.Queues.small, null);
      assertEquals(jobs.size(), 1);
      jobs = Access.listJobs(zk, Access.Queues.small, AccessState.Pending);
      assertEquals(jobs.size(), 0);
      jobs = Access.listJobs(zk, Access.Queues.large, null);
      assertEquals(jobs.size(), 0);
    }

    @Test
    public void accessHappyPathDel() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError{
      load(Tests.access_happy_path_del);
      Access a = Access.createAssembly(zk, Access.Queues.small, token("abc"));
      remap.put("qid0", a.id());
      Access aa = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNotNull(aa);
      assertEquals(a.id(), aa.id());
      assertEquals(aa.status(), AccessState.Pending);
      Access aa2 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa2);
      aa.setStatus(zk, AccessState.Processing);
      assertEquals(aa.status(), AccessState.Processing);
      aa.unlock(zk);

      Access aaa = new Access(Access.Queues.small, a.id());
      aaa.load(zk);
      assertEquals(a.id(), aaa.id());
      aaa.setStatus(zk, aaa.status().success());
      assertEquals(aaa.status(), AccessState.Completed);
      assertTrue(aaa.status().isDeletable());
      aaa.unlock(zk);
      aa2 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa2);

      aaa.delete(zk);
      Access aa3 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa3);
      List<Access> jobs = Access.listJobs(zk, Access.Queues.small, null);
      assertEquals(jobs.size(), 0);
      jobs = Access.listJobs(zk, Access.Queues.large, null);
      assertEquals(jobs.size(), 0);
    }

    @Test
    public void accessFailPath() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError {
      load(Tests.access_fail_path);
      Access a = Access.createAssembly(zk, Access.Queues.small, token("abc"));
      remap.put("qid0", a.id());
      Access aa = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNotNull(aa);
      assertEquals(a.id(), aa.id());
      assertEquals(aa.status(), AccessState.Pending);
      Access aa2 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa2);
      aa.setStatus(zk, AccessState.Processing);
      assertEquals(aa.status(), AccessState.Processing);
      aa.unlock(zk);

      Access aaa = new Access(Access.Queues.small, a.id());
      aaa.load(zk);
      assertEquals(a.id(), aaa.id());
      aaa.setStatus(zk, aaa.status().fail());
      assertEquals(aaa.status(), AccessState.Failed);
      Access aa3 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa3);
      aaa.setStatus(zk, AccessState.Deleted);

      assertTrue(aaa.status().isDeletable());
      aaa.unlock(zk);
      Access aa4 = Access.acquirePendingAssembly(zk, Access.Queues.small);
      assertNull(aa4);
      List<Access> jobs = Access.listJobs(zk, Access.Queues.small, null);
      assertEquals(jobs.size(), 1);
      jobs = Access.listJobs(zk, Access.Queues.small, AccessState.Pending);
      assertEquals(jobs.size(), 0);
      jobs = Access.listJobs(zk, Access.Queues.large, null);
      assertEquals(jobs.size(), 0);
    }

    @Test
    public void findByUuid() throws KeeperException, InterruptedException, MerrittZKNodeInvalid, MerrittStateError {
      load(Tests.create_batch);
      Batch b = Batch.createBatch(zk, fooBar());
      remap.put("bid0", b.id());
      Batch bb = Batch.findByUuid(zk, "bid-uuid");
      assertEquals(b.id(), bb.id());
      Batch bbb = Batch.findByUuid(zk, "bid-uuidXX");
      assertNull(bbb);
    }

}
