package spaxos

type SpaxosLog struct {
	sp  *spaxos
	db  Storager
	net Networker

	chosenIndex uint64
	maxIndex    uint64
}

func (slog *SpaxosLog) Propose(
	reqid uint64, data []byte, asMaster bool) error {

	return slog.sp.propose(reqid, data, asMaster)
}

func (slog *SpaxosLog) MultiPropose(
	reqid uint64, values [][]byte, asMaster bool) error {

	return slog.sp.multiPropose(reqid, values, asMaster)
}
