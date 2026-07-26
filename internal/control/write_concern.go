package control

import "github.com/infamousity/distributed-cache/internal/controlpb"

type WriteConcern int32

const (
	WriteConcernOne      WriteConcern = 0
	WriteConcernMajority WriteConcern = 1
	WriteConcernReplica  WriteConcern = 2
	WriteConcernAll      WriteConcern = 3
)

func toProtoWriteConcern(wc WriteConcern) controlpb.WriteConcern {
	switch wc {
	case WriteConcernMajority:
		return controlpb.WriteConcern_WRITE_CONCERN_MAJORITY
	case WriteConcernReplica:
		return controlpb.WriteConcern_WRITE_CONCERN_REPLICA
	case WriteConcernAll:
		return controlpb.WriteConcern_WRITE_CONCERN_ALL
	default:
		return controlpb.WriteConcern_WRITE_CONCERN_ONE
	}
}

func fromProtoWriteConcern(wc controlpb.WriteConcern) WriteConcern {
	switch wc {
	case controlpb.WriteConcern_WRITE_CONCERN_MAJORITY:
		return WriteConcernMajority
	case controlpb.WriteConcern_WRITE_CONCERN_REPLICA:
		return WriteConcernReplica
	case controlpb.WriteConcern_WRITE_CONCERN_ALL:
		return WriteConcernAll
	default:
		return WriteConcernOne
	}
}
