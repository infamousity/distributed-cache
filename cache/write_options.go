package cache

type WriteConcern int

const (
	WriteConcernOne WriteConcern = iota
	WriteConcernMajority
	WriteConcernAll
)

type CallOptions struct {
	namespace       string
	namespaceSet    bool
	writeConcern    WriteConcern
	writeConcernSet bool
}

type Option func(*CallOptions)

// SetOption and DelOption are aliases for Option to make call sites clearer without
// changing the underlying option system.
type SetOption = Option
type DelOption = Option

func WithNamespace(ns string) Option {
	return func(o *CallOptions) {
		o.namespace = ns
		o.namespaceSet = true
	}
}

func WithWriteConcern(wc WriteConcern) Option {
	return func(o *CallOptions) {
		o.writeConcern = wc
		o.writeConcernSet = true
	}
}
