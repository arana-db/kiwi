# Configuration Change Implementation

## Overview

This document describes the implementation of dynamic cluster configuration changes for the Raft consensus system, including adding and removing nodes from the cluster.

## Implementation Details

### Core Methods

The configuration change functionality is implemented in `src/raft/src/node.rs` with the following key methods:

#### 1. Add Node Safely (`add_node_safely`)

**Purpose**: Safely add a new node to the Raft cluster by first adding it as a learner, waiting for it to catch up, then promoting it to a voting member.

**Process**:
1. Verify the current node is the leader (only leaders can modify membership)
2. Check if the node is already a member or learner
3. Add the node as a learner using `add_learner`
4. Wait for the learner to catch up with the leader's log
5. Promote the learner to a voting member using `change_membership`

**Error Handling**:
- Returns `NotLeader` error if called on a non-leader node
- Gracefully handles attempts to add existing members
- Validates endpoint configuration

#### 2. Remove Node Safely (`remove_node_safely`)

**Purpose**: Safely remove a node from the Raft cluster while maintaining cluster integrity.

**Process**:
1. Verify the current node is the leader
2. Check if the target node is actually a member
3. Validate that we're not removing the last node
4. Remove the node from voting members using `change_membership`
5. Clean up the node's endpoint configuration

**Error Handling**:
- Returns `NotLeader` error if called on a non-leader node
- Prevents removal of the last node in the cluster
- Gracefully handles attempts to remove non-existent nodes

### Supporting Methods

#### `add_learner`
Low-level method to add a node as a non-voting learner. Learners receive log entries but don't participate in voting.

#### `change_membership`
Low-level method to change the cluster's voting membership. This is the core Raft membership change operation.

#### `wait_for_learner_catchup`
Monitors a learner's replication progress and waits until it has caught up with the leader's log (within a configurable threshold).

#### `get_membership` / `get_learners`
Query methods to inspect current cluster membership and learner status.

#### `is_member` / `is_learner`
Helper methods to check if a specific node is a voting member or learner.

## Test Coverage

Comprehensive tests are provided in `src/raft/src/config_change_tests.rs`:

### Test Cases

1. **test_add_node_to_single_node_cluster**
   - Tests adding a second node to a single-node cluster
   - Verifies membership changes correctly

2. **test_add_multiple_nodes_sequentially**
   - Tests adding multiple nodes one at a time
   - Verifies cluster grows correctly to 3 nodes

3. **test_add_node_already_member**
   - Tests idempotency of add operation
   - Verifies graceful handling of duplicate adds

4. **test_remove_node_from_cluster**
   - Tests removing a node from a 3-node cluster
   - Verifies membership shrinks correctly

5. **test_remove_non_member_node**
   - Tests removing a node that isn't in the cluster
   - Verifies graceful handling

6. **test_cannot_remove_last_node**
   - Tests safety check preventing removal of last node
   - Ensures cluster always has at least one member

7. **test_add_node_requires_leader**
   - Verifies only leaders can add nodes
   - Tests proper error handling for non-leaders

8. **test_remove_node_requires_leader**
   - Verifies only leaders can remove nodes
   - Tests proper error handling for non-leaders

9. **test_membership_change_with_direct_API**
   - Tests lower-level membership change APIs
   - Verifies learner promotion workflow

## Usage Examples

### Adding a Node

```rust
// On the leader node
let leader = get_leader_node().await?;

// Add node 4 to the cluster
leader.add_node_safely(4, "127.0.0.1:8004".to_string()).await?;
```

### Removing a Node

```rust
// On the leader node
let leader = get_leader_node().await?;

// Remove node 3 from the cluster
leader.remove_node_safely(3).await?;
```

### Manual Learner Management

```rust
// Add as learner first
leader.add_learner(5, "127.0.0.1:8005".to_string()).await?;

// Wait for catchup
leader.wait_for_learner_catchup(5, Duration::from_secs(30)).await?;

// Promote to voter
let mut members = leader.get_membership().await?;
members.insert(5);
leader.change_membership(members).await?;
```

## Safety Guarantees

1. **Leader-Only Operations**: All membership changes must be initiated by the leader
2. **Learner-First Protocol**: New nodes are added as learners before becoming voters
3. **Catchup Verification**: Learners must catch up before promotion
4. **Minimum Cluster Size**: Cannot remove the last node from the cluster
5. **Idempotent Operations**: Adding existing members or removing non-members is safe

## Requirements Satisfied

This implementation satisfies the following requirements from the specification:

- **Requirement 10.1**: Add nodes to the cluster dynamically
  - Implemented via `add_node_safely` method
  - Supports safe addition with learner-first protocol
  
- **Requirement 10.2**: Remove nodes from the cluster dynamically
  - Implemented via `remove_node_safely` method
  - Includes safety checks to prevent cluster degradation

## Integration Points

The configuration change functionality integrates with:

1. **Network Layer**: Uses `KiwiRaftNetworkFactory` to manage node endpoints
2. **Storage Layer**: Persists membership changes through Raft log
3. **State Machine**: Membership changes are replicated and applied consistently
4. **Cluster Tests**: Used in `cluster_tests.rs` for multi-node cluster setup

## Future Enhancements

Potential improvements for future iterations:

1. **Automatic Rebalancing**: Automatically rebalance data when nodes are added/removed
2. **Joint Consensus**: Implement joint consensus for safer membership changes
3. **Batch Operations**: Support adding/removing multiple nodes in one operation
4. **Health-Based Removal**: Automatically remove persistently unhealthy nodes
5. **Learner Timeout**: Configurable timeout for learner catchup phase

## References

- OpenRaft Documentation: https://docs.rs/openraft/
- Raft Paper: https://raft.github.io/raft.pdf
- Configuration Change Section: §6 of the Raft paper
