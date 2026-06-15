## How to build and run log service alone
### Build

``` sh
mkdir bld && cd bld && cmake .. && make
```

Now you have two executable `launch_sv` and `launch_cl`.

### Run servers

Open three terminals and run three log server as one log group (log group 0).

``` sh
./launch_sv -conf=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -node_id=0 -storage_path=/tmp/ls_raft/raft_data0

./launch_sv -conf=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -node_id=1 -storage_path=/tmp/ls_raft/raft_data1

./launch_sv -conf=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -node_id=2 -storage_path=/tmp/ls_raft/raft_data2
```

### Run client

Open another terminal and run client to access log group 0:

``` sh
./launch_cl -groupid=0 -lg_raft_conf=127.0.0.1:8000:0,127.0.0.1:8001:0,127.0.0.1:8002:0
```
