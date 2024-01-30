package client

type Options struct {
	UseServiceEndpoints bool
}

// Option is an interface for changing configuration in client options.
type Option interface {
	ApplyTo(*Options)
}

var _ Option = (*UseServiceEndpoints)(nil)

// UseServiceEndpoints instructs the client to use the service endpoints instead of endpoints.
type UseServiceEndpoints bool

// ApplyTo applies this configuration to the given options.
func (u UseServiceEndpoints) ApplyTo(opt *Options) {
	opt.UseServiceEndpoints = bool(u)
}

// type optionFunc func(options *Options)
//
// func (f optionFunc) ApplyTo(opt *Options) {
// 	f(opt)
// }
//
// func WithEndpoints(enable bool) Option {
// 	return optionFunc(func(opt *Options) {
// 		opt.UseServiceEndpoints = enable
// 	})
// }

// 选项设计模式
//
//	func InitOptions(opts ...Option) {
//		options := &Options{}
//		for _, opt := range opts {
//			opt.ApplyTo(options)
//		}
//		fmt.Printf("options:%#v\n", options)
//	}
// func main() {
// InitOptions(UseServiceEndpoints(true))
// WithEndpoints(true)
// }
