## Implementation HowTo


1. front-end view:
   - Propose or MultiPropose:
     Trying to propose one write on a single paxos log entry, with one or multiple-value;
     NOTIC: 
     - the proposing value only identify by req-id;
     - block proposing if local believe itself is not up-to-date(minIndex != maxIndex);

   - Get chosen paxos log:
     Front-End ask for new chosen log entry by calling SpaxosLog.Get with know index, and num of log entry expected to read.
     - on the return, hostReqidMap will help to identify if any local proposing has success;
     - Get may return readcnt 0, indicate no progress have been observer, in this case, caller may put itself into wait stat by calling SpaxosLog.Wait

2. spaxos log:
   - remove log id:
     => <logid, index> to unique identify one paxos log: too annoy! (impl perspective)

   - timeout:
     - one simple go-routine generate tick signal one per 1-millisecond
     - spaxos instance update timeoutAt(indicate instance will be timeout At given timeStamp)
     - spaxos: using map[uint64]map[uint64]*spaxosInstance to implement a timeout Queue
       <timeout-timestamp, spaxos instance index, spaxos instance>
     - spaxos will trigger a heart-beat broadcast msg every N millisecond;


    - spaxos instance retire management:
      all spaxos instance below sp.minIndex, will count as retired instance, which also must be a chosen instance.
      Continuous spaxos instance sequence update the nextMinIndex field in spaxos, which will then be passing into storage thread; once storage thread resp with MsgUpdateMinIndex, all spaxos instance with index below the Msg.Indexk can be safely retire.

     - stepChosen:
       - spaxos instance in chosen state will ignore all msg, and response with MsgChosen(non-broadcast);
       - once spaxos instace became chosen, it will try to broadcast a pb.MsgChosen msg;

     - catch up:
       - front-end issue pb.MsgTryCatchUp;
       - spaxos: 
         - for index in (nextMinIndex, min(nextMinIndex+11, maxIndex)), but not yet have corresponding spaxos instance: create a empty spaxos instance, and try to issue a broadcast pb.MsgCatchUp req;
         - for index already in insgroup, the catch up procedure is relayed on timeout setting;
       - spaxos instance: only response pb.MsgCatchUp req if ins is masked as chosen;
       NOTICE: (TOFIX) catch up msg will be broadcast to all peers;

    - propose restriction:
      // FOR MORE GENERAL PROPOSING
      1. add HostPropReqid in pb.HardState: which never change after Propose set up;
      2. HostPropReqid is local propoerty, never broadcast through pb.Message;
      3. 0 == HostPropReqid, indicate it's a no-op proposing, or it's proposing by other spaxos host;
      4. db.Get will return a map describle the relation-ship between host proposing req-id and spaxos log entry, 
         if there are any..
      5. caller should wait for the prev host proposing complete, before issue a new propose;


