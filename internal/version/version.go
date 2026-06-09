package version

import (
	"fmt"
	"time"
)

type Version struct {
	Physical int64
	Logical  uint64
	NodeID   string
}

func Zero() Version {
	return Version{}
}

func FromTime(t time.Time, nodeID string) Version {
	return Version{Physical: t.UnixMilli(), NodeID: nodeID}
}

func (v Version) IsZero() bool {
	return v.Physical == 0 && v.Logical == 0 && v.NodeID == ""
}

func (v Version) Compare(other Version) int {
	if v.Physical < other.Physical {
		return -1
	}
	if v.Physical > other.Physical {
		return 1
	}
	if v.Logical < other.Logical {
		return -1
	}
	if v.Logical > other.Logical {
		return 1
	}
	if v.NodeID < other.NodeID {
		return -1
	}
	if v.NodeID > other.NodeID {
		return 1
	}
	return 0
}

func (v Version) Next(now time.Time, nodeID string) Version {
	next := FromTime(now, nodeID)
	if next.Compare(v) <= 0 {
		next.Physical = v.Physical
		if v.Logical == ^uint64(0) {
			next.Physical++
			next.Logical = 0
		} else {
			next.Logical = v.Logical + 1
		}
		next.NodeID = nodeID
	}
	return next
}

func (v Version) String() string {
	if v.IsZero() {
		return "0"
	}
	return fmt.Sprintf("%d/%d/%s", v.Physical, v.Logical, v.NodeID)
}
