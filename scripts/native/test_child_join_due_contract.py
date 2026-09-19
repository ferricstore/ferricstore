from __future__ import annotations

import os
import time
import uuid

import pytest

from ferricstore import ChildSpec, FlowClient

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run native integration tests",
)


@pytest.mark.parametrize(
    ("outcome", "successor"),
    [
        ("complete", "queued"),
        ("complete", "page_ready"),
        ("fail", "repair_ready"),
        ("complete", "completed"),
        ("fail", "failed"),
        ("fail", "cancelled"),
    ],
)
def test_resolved_parent_claimability_matches_successor(
    outcome: str, successor: str
) -> None:
    client = FlowClient.from_url(os.environ["FERRICSTORE_URL"])
    suffix = uuid.uuid4().hex
    parent_type = f"join-parent-{suffix}"
    child_type = f"join-child-{suffix}"
    partition = f"join-{suffix}"
    parent_id = f"parent-{suffix}"
    child_id = f"child-{suffix}"
    now = int(time.time() * 1000)

    try:
        client.create(
            parent_id,
            type=parent_type,
            state="queued",
            partition_key=partition,
            now_ms=now,
            run_at_ms=now,
        )
        [parent] = client.claim_flows(
            parent_type,
            state="queued",
            worker="join-parent",
            partition_key=partition,
            now_ms=now,
        )
        client.spawn_children(
            parent_id,
            [ChildSpec(child_id, child_type)],
            partition_key=partition,
            lease_token=parent.lease_token,
            fencing_token=parent.fencing_token,
            wait="all",
            wait_state="waiting_children",
            success=successor,
            failure=successor,
            on_child_failed="fail_parent",
            now_ms=now + 1,
        )
        [child] = client.claim_flows(
            child_type,
            state="queued",
            worker="join-child",
            partition_key=partition,
            now_ms=now + 2,
        )
        settle = client.complete if outcome == "complete" else client.fail
        settle(
            child.id,
            partition_key=partition,
            lease_token=child.lease_token,
            fencing_token=child.fencing_token,
            now_ms=now + 3,
        )
        record = client.get(parent_id, partition_key=partition)
        assert record is not None
        assert record.state == successor
        assert record.lease_token == b""
        assert record.raw is not None
        attempts = record.raw.get("attempts", record.raw.get(b"attempts"))
        assert attempts == 0
        resumed = client.claim_flows(
            parent_type,
            state=successor,
            worker="join-resumed-parent",
            partition_key=partition,
            now_ms=now + 3,
        )
        if successor in {"completed", "failed", "cancelled"}:
            assert resumed == []
            return
        assert [job.id for job in resumed] == [parent_id]
        assert resumed[0].fencing_token > parent.fencing_token
        client.complete(
            resumed[0].id,
            partition_key=partition,
            lease_token=resumed[0].lease_token,
            fencing_token=resumed[0].fencing_token,
            now_ms=now + 4,
        )
    finally:
        client.close()
