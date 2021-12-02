package stream

import (
	"net"
	"net/url"
)

// Addr is an AMQP stream address.
type Addr struct {
	// url is the parsed URL.
	url *url.URL

	// values are the parsed parameters from url.
	values url.Values
}

// Network implements net.Addr.Network.
func (a *Addr) Network() string {
	return a.url.Scheme
}

// String implements net.Addr.String.
func (a *Addr) String() string {
	return a.url.String()
}

// ServerQueueName returns the server queue name.
// If this address does not have a server queue name,
// a net.InvalidAddrError is returned instead.
func (a *Addr) ServerQueueName() (string, error) {
	serverQueueName := a.values.Get("server_queue")
	if serverQueueName == "" {
		return "", net.InvalidAddrError("server_queue missing from URL")
	}
	return serverQueueName, nil
}

// setServerQueueName returns a copy of this address with the given server
// queue name set.
func (a *Addr) setServerQueueName(name string) *Addr {
	return a.set("server_queue", name)
}

// local returns a copy of this address with the given local queue name set.
func (a *Addr) local(localQueueName string) *Addr {
	return a.set("local_queue", localQueueName)
}

// remote returns a copy of this address with the given remote queue name set.
func (a *Addr) remote(remoteQueueName string) *Addr {
	return a.set("remote_queue", remoteQueueName)
}

// set returns a copy of this address, with the given key set to value in
// the query part of the URL.
func (a *Addr) set(key, value string) *Addr {
	urlCopy := *a.url
	valuesCopy := make(url.Values, len(a.values)+1)
	for k, v := range a.values {
		valuesCopy[k] = v
	}
	valuesCopy.Set(key, value)
	urlCopy.RawQuery = valuesCopy.Encode()
	return &Addr{
		url:    &urlCopy,
		values: valuesCopy,
	}
}

// NewAddr creates a new address from the given URL string.
func NewAddr(urlString string) (*Addr, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return nil, &net.ParseError{
			Type: "AMQP URL",
			Text: urlString,
		}
	}
	if parsedURL.User != nil {
		// scratch password
		parsedURL.User = url.User(parsedURL.User.Username())
	}
	values, err := url.ParseQuery(parsedURL.RawQuery)
	if err != nil {
		return nil, &net.ParseError{
			Type: "AMQP URL query",
			Text: parsedURL.RawQuery,
		}
	}
	return &Addr{
		url:    parsedURL,
		values: values,
	}, nil
}
