# Leader Transfer

## Introduction

Leader transfer happens when a node that is the preferred leader of some node groups recovers. Suppose a cluster of three nodes, `node0`, `node1` and `node2` with three node groups `ng0`, `ng1` and `ng2`. The preferred leader of `ng{i}` is `node{i}`. When `node0` fails, the leadership of `ng0` will go to `node1` or `node2`. Leader transfer happens when `node0` recovers and takes the leadership of `ng0` back. The sender of the leader transfer message is the node that wants to take the leadership back. The receiver is the current leader. In this case, the sender is `node0`, and the receiver is `node1`.

## Implementation

The contents of the request and response are as follows.

```
TransferRequest {
  uint32 ng_id;
  uint32 node_id;
  int64 term;
}
```
`ng_id` tells the id of the node group, in this case, `ng0`.

`node_id` is the sender's id, in this case, `0` (`node_0`).

`term` is the current term, used for the receiver to check whether the request is not outdated.

```
TransferResponse {
  int32 error_code;
}

error_code:
Ok, NotLeader, TermOutdated, Error
```

The request is sent in `on_start_following(ctx)` in braft. `on_start_following(ctx)` is called when a node becomes a follower. `ctx` stores enough information including term and leader in this term.

The sender will start a thread to keep sending requests to the leader. The term in `ctx` will be stored as a local variable `term` in the thread. The current term of the node will also be stored as `current_term_`.  When the next `on_start_following(ctx)` arrives, `current_term_` will be updated to notify the previous thread (if exists) to terminate.

The rules of sender:

1. Update `current_term_` with `ctx.term`;
2. Send a request to the leader;
3. If the network fails and `term == current_term_`, go to step 2;
4. If the error code is `Error` and `term == current_term_`, go to step 2, otherwise, finish.

The rules of receiver:

1. If its current term is greater than `request.term`, return `TermOutdated`; (Term checking)
2. If it is not leader now, return `NotLeader`; (Leader checking )
3. call `transfer_leadership_to()` in braft, and return `Ok` if no error or `Error`.

Functions such as `on_start_following()` and `on_leader_start()` are executed asynchronously in braft. 

Term checking and leader checking are used to verify the request to avoid the dead loop of the sender.

Term checking is useful when `on_start_following(ctx.term=1, ctx.leader=1)` is invoked when `node1`'s term is 2.
Leader checking is useful when `on_start_following(ctx.term=1, ctx.leader=1)` is invoked when `node1` and `node2`'s terms are 2 and the leader is `node2`.

