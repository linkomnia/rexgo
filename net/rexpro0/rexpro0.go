// Copyright (c) 2013 LinkOmnia Limited. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file.

/*
Package rexpro0 implements the RexPro version 0 protocol.

See:
https://github.com/tinkerpop/rexster/wiki/RexPro-Messages/46f76da31149b2a3a68fb470d28157e23739a66d

This package is heavily inspired by the package net/rpc.
*/
package rexpro0

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ugorji/go/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

var ErrUnknownVersion = errors.New("unknown protocol version")
var ErrShutdown = errors.New("connection is shut down")
var EmptySession [16]byte = [16]byte{}

var mh codec.MsgpackHandle

func init() {
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
	mh.RawToString = true
}

type MetaMap map[string]interface{}

// Call represents an active request.
type Call struct {
	Session      [16]byte
	RequestType  MsgType
	RequestMeta  MetaMap
	RequestArgs  []interface{}
	ResponseType MsgType
	ResponseMeta MetaMap
	ResponseArgs []interface{}
	Error        error
	Done         chan *Call
}

// Client represents a RexPro Client.
// There may be multiple outstanding Calls associated with a single Client, and a Client may be used by multiple goroutines simultaneously.
type Client struct {
	mutex    sync.Mutex // protects pending, seq, request
	sending  sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
	closing  bool
	shutdown bool
	conn     io.ReadWriteCloser
	rbuf     *bufio.Reader
	wbuf     *bufio.Writer
}

type Version byte // must be 0
type MsgType byte

const (
	MsgError MsgType = iota
	MsgSessionRequest
	MsgSessionResponse
	MsgScriptRequest
	MsgScriptResponse
)

type MsgHeader struct {
	Version  Version
	Type     MsgType
	BodySize uint32
}

const (
	_ = iota
	ChannelConsole
	ChannelMsgPack
	ChannelGraphSON
)

func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	client.mutex.Lock()
	if client.shutdown || client.closing {
		call.Error = ErrShutdown
		client.mutex.Unlock()
		call.done()
		return
	}
	seq := client.seq
	client.seq++
	client.pending[seq] = call
	client.mutex.Unlock()

	// encode and send the request
	err := client.writeMessage(call.RequestType, call.Session, seq, call.RequestMeta, call.RequestArgs)
	if err != nil {
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) writeMessage(typ MsgType, session [16]byte, seq uint64, meta MetaMap, args []interface{}) (err error) {
	// turn seq into 16 bytes
	var seqBytes [16]byte
	binary.BigEndian.PutUint64(seqBytes[8:], seq)

	// message body as msgpack list
	msg := []interface{}{&session, &seqBytes, meta}
	msg = append(msg, args...)
	var msgBytes []byte
	enc := codec.NewEncoderBytes(&msgBytes, &mh)
	if err = enc.Encode(msg); err != nil {
		return
	}

	// fill in the rest of the header
	h := &MsgHeader{0, typ, uint32(len(msgBytes))}
	if err = binary.Write(client.wbuf, binary.BigEndian, h); err != nil {
		return
	}

	if _, err = client.wbuf.Write(msgBytes); err != nil {
		return
	}

	return client.wbuf.Flush()
}

func (client *Client) readMessage() (typ MsgType, session [16]byte, seq uint64, meta MetaMap, args []interface{}, err error) {
	h := &MsgHeader{}
	if err = binary.Read(client.rbuf, binary.BigEndian, h); err != nil {
		return
	}
	if h.Version != 0 {
		err = ErrUnknownVersion
		return
	}
	typ = h.Type
	bodybuf := make([]byte, h.BodySize)
	if _, err = io.ReadFull(client.rbuf, bodybuf); err != nil {
		return
	}
	var body []interface{}
	dec := codec.NewDecoderBytes(bodybuf, &mh)
	if err = dec.Decode(&body); err != nil {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			switch e.(type) {
			case error:
				err = e.(error)
			default:
				err = errors.New("unexpected type during decoding")
			}
		}
	}()
	copy(session[:], []byte(body[0].(string)))
	seq = binary.BigEndian.Uint64(([]byte(body[1].(string)))[8:])
	meta = body[2].(map[string]interface{})
	args = body[3:]
	return
}

func (client *Client) input() {
	var err error
	for err == nil {
		typ, session, seq, meta, args, err := client.readMessage()
		if err != nil {
			break
		}

		client.mutex.Lock()
		call := client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()

		if call == nil {
			if err != nil {
				err = errors.New("reading error body: " + err.Error())
			}
			break
		}
		call.ResponseType = typ
		switch typ {
		case MsgError:
			flag, ok := meta["flag"].(int)
			if !ok {
				flag = ErrFlagUnknown
			}
			errmsg, ok := args[0].(string)
			if !ok {
				errmsg = "unknown server error"
			}
			call.Error = &RexsterError{flag, errmsg}
		case MsgSessionResponse:
			call.Session = session
			fallthrough
		default:
			call.ResponseMeta = meta
			call.ResponseArgs = args
		}
		call.done()
	}
	// Terminate pending calls
	client.sending.Lock()
	client.mutex.Lock()
	client.shutdown = true
	closing := client.closing
	if err == io.EOF {
		if closing {
			err = ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
	client.mutex.Unlock()
	client.sending.Unlock()
	if err != io.EOF && !closing {
		log.Print("rexpro0: client protocol error: ", err)
	}
}

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here.  It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		log.Println("rexpro0: discarding Call reply due to insufficient Done chan capacity")
	}
}

func (client *Client) Close() error {
	client.mutex.Lock()
	if client.shutdown || client.closing {
		client.mutex.Unlock()
		return ErrShutdown
	}
	client.closing = true
	client.mutex.Unlock()
	return client.conn.Close()
}

// Dial connects to a RexPro server at the specified network address.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

// NewClient returns a new Client to handle requests to the set of services at
// the other end of the connection. It adds a buffer to both the write and read
// side of the connection so the header and body are sent and received as a
// unit.
func NewClient(conn io.ReadWriteCloser) *Client {
	client := &Client{
		pending: make(map[uint64]*Call),
		conn:    conn,
		rbuf:    bufio.NewReader(conn),
		wbuf:    bufio.NewWriter(conn),
		seq:     1,
	}
	go client.input()
	return client
}

// Go invokes the function asynchronously. It returns the Call structure
// representing the invocation. The done channel will signal when the call is
// complete by returning the same Call object. If done is nil, Go will allocate
// a new channel. If non-nil, done must be buffered or Go will deliberately
// crash.
func (client *Client) Go(session [16]byte, typ MsgType, meta MetaMap, args []interface{}, done chan *Call) *Call {
	call := new(Call)
	call.Session = session
	call.RequestType = typ
	call.RequestMeta = meta
	call.RequestArgs = args
	if done == nil {
		done = make(chan *Call, 10)
	} else {
		if cap(done) == 0 {
			log.Panic("rexpro0: done channel is unbuffered")
		}
	}
	call.Done = done
	client.send(call)
	return call
}

// Call sends the request to the server, waits for it to complete, and returns
// its results.
func (client *Client) Call(session [16]byte, typ MsgType, meta MetaMap, args []interface{}) ([16]byte, MsgType, MetaMap, []interface{}, error) {
	call := <-client.Go(session, typ, meta, args, make(chan *Call, 1)).Done
	return call.Session, call.ResponseType, call.ResponseMeta, call.ResponseArgs, call.Error
}

// RexsterError represents an Error Response returned by the RexPro server.
type RexsterError struct {
	Flag    int
	Message string
}

func (e RexsterError) Error() string {
	return fmt.Sprintf(`%s [%d]`, e.Message, e.Flag)
}

const (
	ErrFlagMalformedMessage = iota
	ErrFlagSessionNotFound
	ErrFlagScriptExecFailed
	ErrFlagInvalidCredentials
	ErrFlagGraphNotFound
	ErrFlagChannelNotFound
	ErrFlagSerializationFailed
	ErrFlagUnknown = -1
)
