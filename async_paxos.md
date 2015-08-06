
1. db过来的提议请求, 必然包涵{req_id, value}, 由db负责req_id的唯一;
2. db提请求是个异步的过程, 提出请求后, 只能通过chosen queue来确认对应请求是否被chosen了; 
3. chosen queue返回数据中包涵index信息, db需要按index顺序apply;

### StateMachine Thread(SMT)

- OUTPUT QUEUE
  1. CHOSEN QUEUE: {index, req_id, value}+, 对接前端DB
     当index确定后, 对应值将放入这个队列中;

  2. STORAGE STATE + MSG QUEUE: {hard_state + msgs}+, 对接Storage Thread
     - hard_state: 需要固化到磁盘上的状态信息{proposer_num, promised_num, accepted_num, accepted_value};
     - msgs: 当hard_state成功固化到磁盘后, 可以通过网络转发出去的信息;

- INPUT QUEUE
  1. PROPOSING QUEUE: {req_id, value}+, 对接前端DB
     前端将带写的数据放入队列; SMT根据当前max_index, 依次分配{index, req_id, value}, 建立对应状态机;

  2. RECVMSG QUEUE: {msgs}+, 对接Network Thread
     msgs中的消息分为:
     - 来自网络的paxos消息;
     - ST发来包涵重构某个旧的index对应的状态机的消息; 

SMT根据RECVMSG QUEUE中的消息推动对应状态机, 生成{hard_stat + msgs}+, 而后放到STORAGE STATE + MSG QUEUE中; 

### Storage Thread(ST)

- OUTPUT QUEUE
  1. MSG QUEUE: {msgs}+, 对接NetworkThread
     待发送的消息队列; 

- INPUT QUEUE
  1. STORAGE STATE + MSG QUEUE: {hard_state + msgs}+, 对接StateMachine Thread

ST负责将STORAGE STATE + MSG QUEUE中的hard_state固化到磁盘上, 并在成功固化后将对应的msgs放入MSG QUEUE; 
另外, ST还负责处理msgs中标记为重构状态机的消息类型: 即从磁盘中读取某index对应的状态机信息, 封装成msg结构, 最终将放入MSG QUEUE中, 由NT负责转交SMT; 此外, 当某次hard_state保存失败时, 对应的msgs将被丢弃, 同时ST将试图从磁盘中加在对应的旧状态机; 

#### Network Thread(NT)

- OUTPUT QUEUE
  1. RECVMSG QUEUE: {msgs}+, 对接StateMachine Thread
     NT负责将从网络中收到的paxos消息, 以及由ST转发的消息递交给SMT;

- INPUT QUEUE
  1. MSG QUEUE: {msgs}+, 对接Storage Thread
     NT负责将消息转到RECVMSG QUEUE中, 或者通过网络发送出去; 


