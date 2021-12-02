package stream

import (
	"net"
	"net/url"
)

// addr is an AMQP stream address.
type addr struct {
	// url is the parsed URL.
	url *url.URL

	// values are the parsed parameters from url.
	values url.Values
}

// Network implements net.Addr.Network.
func (a *addr) Network() string {
	return a.url.Scheme
}

// String implements net.Addr.String.
func (a *addr) String() string {
	return a.url.String()
}

// serverQueueName returns the server queue name.
func (a *addr) serverQueueName() (string, error) {
	serverQueueName := a.values.Get("server_queue")
	if serverQueueName == "" {
		return "", net.InvalidAddrError("server_queue missing from URL")
	}
	return serverQueueName, nil
}

// setServerQueueName returns a copy of this address with the given server
// queue name set.
func (a *addr) setServerQueueName(name string) *addr {
	return a.set("server_queue", name)
}

// local returns a copy of this address with the given local queue name set.
func (a *addr) local(localQueueName string) *addr {
	return a.set("local_queue", localQueueName)
}

// remote returns a copy of this address with the given remote queue name set.
func (a *addr) remote(remoteQueueName string) *addr {
	return a.set("remote_queue", remoteQueueName)
}

// set returns a copy of this address, with the given key set to value in
// the query part of the URL.
func (a *addr) set(key, value string) *addr {
	urlCopy := *a.url
	valuesCopy := make(url.Values, len(a.values)+1)
	for k, v := range a.values {
		valuesCopy[k] = v
	}
	valuesCopy.Set(key, value)
	urlCopy.RawQuery = valuesCopy.Encode()
	return &addr{
		url:    &urlCopy,
		values: valuesCopy,
	}
}

// newAddr creates a new address from the given URL string.
func newAddr(urlString string) (*addr, error) {
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
	return &addr{
		url:    parsedURL,
		values: values,
	}, nil
}
