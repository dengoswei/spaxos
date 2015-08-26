# Sipmle Paxos
branch
- raft_spaxos: first version of my simple paxos, raft-like(etcd/raft) async message-driven statemachine;
- master: rewrite(as doc below), working;

## Playaround Tools

``` bash
./simplesvr config_file ip:port
```

NOTICE: to make sure progress, you should setting up at least more then half svr(as descript in config_file)

``` bash
telnet ip:port
PROP $(reqid) $(prop-value)
```

NOTICE: svr don't echo back anything, even if given propose have been chosen; check out svr side log instead;

## Feature List
1. spaxos server as a paxos log lib, the comm usage as shown in simplesvr example;
2. current implementation don't support: config change, master propose, no-op propose yet;
    master propose & no-op porpose are in the TODO list; but config change probably not!
3. (TODO)

## Assumption
1. a proposing request will contain req-id + req-value; the req-id need to be unique;
2. spaxos don't gurante a propsing request will eventually be chosen. client can only
   sure propsing request been chosen by checking the chosen item return by spaxos with
   req-id match propsing request;
3. spaxos don't guarrente the return order of chosen item;

### StateMachine Thread(SMT)

- OUTPUT QUEUE
  1. CHOSEN QUEUE: {index, req-id, req-value}+, attach to client(db maybe)
     once a spaox instance been chosen, SMT will put {index, req-id, req-value} into chosen queue;

  2. STORAGE STATE + MSG QUEUE: {hard-state + msgs}+, attach to Storage Thread
     - hard-state: data need to be store persistently, {proposer_num, promised_num, accepted_num, accepted_value};
     - msgs: which will only be send after hard_state have been safely save to storage;

- INPUT QUEUE
  1. PROPOSING QUEUE: {req-id, value}+, attach to client
     client submit propsing request into proposing queue. SMT will then assign a new index number, 
     which also indicate creating a new spaxos instance;

  2. RECVMSG QUEUE: {msgs}+, attach to Network Thread
     there are two-type of msg:
     - spaxos msg recving from network;
     - ctrl msg: timeout, spaxos instance rebuild, and so on..

All msgs from recvmsg queue will be feeded into corresponding spaxos instace, which in turn produce {hard-state + msg}+.


### Storage Thread(ST)

- OUTPUT QUEUE
  1. MSG QUEUE: {msgs}+, attach to Network Thread
     msgs wait to send or transfer;

- INPUT QUEUE
  1. STORAGE STATE + MSG QUEUE: {hard_state + msgs}+, attach to StateMachine Thread

ST will try save hard-state into perisitent storage, and only forward msgs into msg queue after a success write.
In case of write error or recving a rebuild msg, ST will try to read previous persistent spaxos instace info, which will then be passed into SMT to rebuild spaox instance;

#### Network Thread(NT)

- OUTPUT QUEUE
  1. RECVMSG QUEUE: {msgs}+, attach to StateMachine Thread
     all the msgs from spaxos peers will be gathered by network thread, and then forward into SMT;

- INPUT QUEUE
  1. MSG QUEUE: {msgs}+, attach to Storage Thread
     msgs from msg queue will serve two purpose:
     - sending to spaox peers;
     - forward into SMT;

