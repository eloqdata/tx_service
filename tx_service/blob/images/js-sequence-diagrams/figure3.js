TxProcessor->Checkpointer: Ccmap full, start ckpt
Note right of Checkpointer: Timeout based ckpt
Checkpointer->TxProcessor: Sned CkptTsCc request to get ckpt_ts
TxProcessor->Checkpointer: Return ckpt_ts
Checkpointer->TxProcessor: Send CkptScanCc request for each ccmap
Note right of TxProcessor: CkptScanCc will copy payload_\n to payload_ckpt_
TxProcessor->Checkpointer: Return ckpted_ccentry_list
Checkpointer->Cassandra: flush data in ckpted_ccentry_list\n to data store
Note right of Checkpointer: update the ckpt_ts_\n of ckpted ccentries
