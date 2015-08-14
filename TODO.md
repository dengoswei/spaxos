1. add: db req the value of given index ?
   : GetChosen may not return chosen item index by index; 
     But db must apply the log item extactly by index(inc order);
     Dis-continutes index sequence will delay db commit chosen item;

2. add: prevent Propose Req if local is not up-to-date;

3. majority read:(mega-store)
   "read from a majority of replicas to find the maximum log position
   that any replica has seen, and pick a replica to read from;"
   
   => req the max seen log posistion + commited position;

4. db: client req with a max seen log posistion:
   "Catch Up"
   => db req spaxos log up-to-date to as-least this point/or exactly?
      then process req; // read

   "For any log positions without a known-committed value available, 
   invoke Paxos to propose a no-op write."
   // => 

5. add timeout! & prop num 0 support & nil(no-op) prop value!
   => random backoff if accept pharse or prepare pharse failed

6. different propose:
   1. db normal prop;
   2. db master prop(prop num 0 & skip prepare pharse);
   3. db req chosen item of index i, (by prop a no-op prop value)
      => try to wait for all rsp util timeout, pick a majority(fav nip op)

7. "If the chosen value differs from that originally proposed, return a
    conflict error."
   => each prop req associate with a communicate chan ? 

