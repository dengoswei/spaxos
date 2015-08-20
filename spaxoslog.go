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
	data map[uint64][]byte, asMaster bool) error {

	return slog.sp.multiPropose(data, asMaster)
}
