# Raft Host Manager

A host manager implemented with braft. Host manager will be responsible for controlling failover of tx service. It will be forked on tx service start up, and exit when
parent tx service process is gone.