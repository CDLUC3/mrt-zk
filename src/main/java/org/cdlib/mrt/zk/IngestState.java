package org.cdlib.mrt.zk;

import java.util.List;
import org.json.JSONObject;

/**
 * Common interface for Ingest Queue State Enums
 */
public interface IngestState {
  public List<IngestState> nextStates();
  public String name();
  public IngestState stateChange(IngestState next);

  default boolean isDeletable() {
    return nextStates().isEmpty();
  }
  default boolean stateChangeAllowed(IngestState next) {
    return nextStates().contains(next);
  }
  default IngestState success() {
    return null;
  }
  default IngestState fail() {
    return null;
  }
  public static JSONObject statesAsJson(IngestState[] values) {
    JSONObject j = new JSONObject();
    for(IngestState v: values) {
      JSONObject jnext = new JSONObject();
      for(IngestState nv: v.nextStates()){
        JSONObject t = new JSONObject();
        if (nv.equals(v.success())) {
          t.put("success", true);
        }
        if (nv.equals(v.fail())) {
          t.put("fail", true);
        }
        jnext.put(nv.name(), t.isEmpty() ? JSONObject.NULL : t);
      }
      j.put(v.name(), jnext.isEmpty() ? JSONObject.NULL : jnext);
    }
    return j;
  }

}
