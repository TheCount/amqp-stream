package stream

import (
	"crypto/tls"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// opts describes AMQP stream options.
type opts struct {
	// insecure is set by WithInsecure.
	insecure bool

	// tlsConfig is set by WithTLSConfig.
	tlsConfig *tls.Config

	// auth is set by WithExternalAuth.
	// If auth is nil, plain authentication will be used.
	auth amqp.Authentication
}

// Validate validates these options.
func (o *opts) Validate() error {
	if o.tlsConfig == nil && !o.insecure {
		return errors.New("one of WithInsecure and WithTLSConfig must be specified")
	}
	return nil
}

// Option is an AMQP stream option. Options are returned by the With… functions.
type Option func(o *opts) error

// WithInsecure indicates that no TLS configuration should be used when
// connecting to the AMQP server. This means either a plaintext connection
// (for amqp:// URLs) or server verification only (for amqps:// URLs).
func WithInsecure() Option {
	return func(o *opts) error {
		if o.tlsConfig != nil {
			return errors.New(
				"WithInsecure and WithTLSConfig are mutually incompatible")
		}
		o.insecure = true
		return nil
	}
}

// WithTLSConfig sets the TLS configuration for the connection to the AMQP
// server. After a call to WithTLSConfig, config must not be changed.
func WithTLSConfig(config *tls.Config) Option {
	return func(o *opts) error {
		if config == nil {
			return errors.New("TLS config is nil")
		}
		if o.tlsConfig != nil {
			return errors.New("WithTLSConfig specified multiple times")
		}
		if o.insecure {
			return errors.New(
				"WithTLSConfig and WithInsecure are mutually incompatible")
		}
		o.tlsConfig = config
		return nil
	}
}

// WithExternalAuth enables external authentication (e. g., via TLS credentials)
// with the AMQP server. By default, plain authentication is used.
func WithExternalAuth() Option {
	return func(o *opts) error {
		if o.auth != nil {
			return errors.New("WithExternalAuth: authentication method already set")
		}
		o.auth = &amqp.ExternalAuth{}
		return nil
	}
}
