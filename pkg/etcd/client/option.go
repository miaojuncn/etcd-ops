package client

type Options struct {
	UseServiceEndpoints bool
}

// Option is an interface for changing configuration in client options.
type Option interface {
	ApplyTo(*Options)
}

// var _ Option = (*UseServiceEndpoints)(nil)

// UseServiceEndpoints instructs the client to use the service endpoints instead of endpoints.
type UseServiceEndpoints bool

// ApplyTo applies this configuration to the given options.
func (u UseServiceEndpoints) ApplyTo(opt *Options) {
	opt.UseServiceEndpoints = bool(u)
}
