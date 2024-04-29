package backend

import (
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// This file contains hand-written extensions to the types defines in common.pb.go

// ToTimeStamp converts a Delay to a Protobuf Timestamp object.
// The parameter "base" indicates the base/current time and it's used when the delay is relative.
func (x *Delay) ToTimeStamp(base time.Time) *timestamppb.Timestamp {
	if x == nil {
		return nil
	}

	switch d := x.GetDelayed().(type) {
	case *Delay_Delay:
		return timestamppb.New(base.Add(d.Delay.AsDuration()))
	case *Delay_Time:
		return d.Time
	default:
		return nil
	}
}
