## Implementation HowTo


1. front-end view:
   - Propose or MultiPropose:
     Trying to propose one write on a single paxos log entry, with one or multiple-value;
     NOTIC: the proposing value only identify by req-id;

   - Get chosen paxos log:
     Front-end may ask for paxos log entry on given position(log index), spaxos log will answer
     this request only if log index and all index preceding it have been mark chosen;

2. spaxos log:
   - remove log id:
     => <logid, index> to unique identify one paxos log: too annoy! (impl perspective)

   - timeout:
     - one simple go-routine generate tick signal one per 1-millisecond
     - spaxos instance update timeoutAt(indicate instance will be timeout At given timeStamp)
     - spaxos: using map[uint64]map[uint64]*spaxosInstance to implement a timeout Queue
       <timeout-timestamp, spaxos instance index, spaxos instance>


    - spaxos instance retire management:
      A chosen spaxos instance will put itself into retire queue. 
      Continuous spaxos instance sequence update the nextMinIndex field in spaxos, which will then be passing into storage thread; once storage thread resp with MsgUpdateMinIndex, all spaxos instance with index below the Msg.Indexk can be safely retire.


     - stepChosen:
       spaxos instance in chosen state will ignore all msg, and response with MsgChosen(non-broadcast);



