// Copyright (c) 2013 LinkOmnia Limited. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file.

/*
Package rexgo is a Rexster client for Go using the binary RexPro protocol.
The actual RexPro version 0 wire protocol is implemented in the package
"net/rexpro0".

Note that only RexPro version 0 is implemented at this time. This means Rexster
2.4.0 and newer are not supported.

This package uses the excellent codec from ugorji for MsgPack serialization:
https://github.com/ugorji/go/tree/master/codec

For more information about the Rexster graph database and the RexPro protocol,
see: https://github.com/tinkerpop/rexster/wiki
*/
package rexgo

// BUG(roger.so): RexPro version 1 is not implemented.

import (
	"errors"
	"github.com/linkomnia/rexgo/net/rexpro0"
	"sync"
)

var (
	ErrGraphNameDefinedForSession = errors.New("rexgo: A graph name has already been defined on this session")
)

// Client represents a Rexster Client. A Client may be used by multiple
// goroutines simultaneously.
type Client struct {
	GraphName    string // The default name of the graph to work on. Optional.
	GraphObjName string // The variable name of the graph object specified by GraphName. Defaults to "g".
	rx           *rexpro0.Client
	mutex        sync.Mutex
}

// Dial connects to a Rexster server at the specified network address.
func Dial(addr string) (*Client, error) {
	rx, err := rexpro0.Dial(addr)
	if err != nil {
		return nil, err
	}
	client := &Client{
		rx: rx,
	}
	return client, nil
}

func (client *Client) Close() {
	client.rx.Close()
}

func (client *Client) SetGraphName(graphName string) {
	client.mutex.Lock()
	client.GraphName = graphName
	client.mutex.Unlock()
}

func (client *Client) SetGraphObjName(objName string) {
	client.mutex.Lock()
	client.GraphObjName = objName
	client.mutex.Unlock()
}

func (client *Client) newMetaMap() rexpro0.MetaMap {
	meta := make(rexpro0.MetaMap)

	client.mutex.Lock()
	gname := client.GraphName
	oname := client.GraphObjName
	client.mutex.Unlock()

	if gname != "" {
		meta["graphName"] = gname
	}
	if oname != "" {
		meta["graphObjName"] = oname
	}

	return meta
}

// Session represents a session opened on the Rexster server.
type Session struct {
	GraphName           string // The default name of the graph to work on. Optional.
	GraphObjName        string // The variable name of the graph object specified by GraphName. Defaults to "g".
	mutex               sync.Mutex
	graphNameDefined    bool
	graphObjNameDefined bool
	client              *Client
	Id                  [16]byte // The UUID for the Session as assigned by the server.
}

// NewSession is a convenience function for opening a session when authentication is not required. See NewSessionWithAuth.
func (client *Client) NewSession() (*Session, error) {
	return client.NewSessionWithAuth("", "")
}

// NewSessionWithAuth asks the server to establish a new session. Variable bindings are preserved across Script requests within an open session.
func (client *Client) NewSessionWithAuth(username string, password string) (session *Session, err error) {
	meta := client.newMetaMap()
	args := []interface{}{rexpro0.ChannelMsgPack, username, password}

	sid, _, _, _, err := client.rx.Call(rexpro0.EmptySession, rexpro0.MsgSessionRequest, meta, args)
	if err != nil {
		return nil, err
	}

	session = &Session{
		Id:                  sid,
		client:              client,
		graphNameDefined:    (meta["graphName"] != ""),
		graphObjNameDefined: (meta["graphObjName"] != ""),
	}
	return session, nil
}

func (s *Session) Close() error {
	meta := s.client.newMetaMap()
	meta["killSession"] = true
	args := []interface{}{rexpro0.ChannelMsgPack, "", ""}

	s.mutex.Lock()
	sid := s.Id
	s.mutex.Unlock()
	_, _, _, _, err := s.client.rx.Call(sid, rexpro0.MsgSessionRequest, meta, args)
	return err
}

// Script sends the Gremlin script to the Rexster server, waits for it to complete, and returns the results.
//
// Example:
//  bindings := map[string]interface{}{"godname": "saturn"}
//  obj, err := rx.Script(`g.V('name',godname).in('father').in('father').name`, bindings)
//  result := obj.([]interface{})
//  if result[0] != "hercules" {
//      // uh-oh
//  }
func (client *Client) Script(script string, bindings map[string]interface{}) (results interface{}, err error) {
	meta := client.newMetaMap()
	args := []interface{}{"groovy", script, bindings}

	_, _, _, args, err = client.rx.Call(rexpro0.EmptySession, rexpro0.MsgScriptRequest, meta, args)
	if err != nil {
		return nil, err
	}

	return args[0], nil
}

func (s *Session) newMetaMap() rexpro0.MetaMap {
	meta := make(rexpro0.MetaMap)

	s.mutex.Lock()
	gname := s.GraphName
	oname := s.GraphObjName
	s.mutex.Unlock()

	if gname != "" {
		meta["graphName"] = gname
	}
	if oname != "" {
		meta["graphObjName"] = oname
	}

	meta["inSession"] = true
	meta["isolate"] = false
	return meta
}

// Script sends the Gremlin script to the Rexster server, waits for it to
// complete, and returns the results. Variable bindings are preserved in the
// same session.
func (s *Session) Script(script string, bindings map[string]interface{}) (results interface{}, err error) {
	meta := s.newMetaMap()
	args := []interface{}{"groovy", script, bindings}

	s.mutex.Lock()
	sid := s.Id
	s.mutex.Unlock()
	_, _, _, args, err = s.client.rx.Call(sid, rexpro0.MsgScriptRequest, meta, args)
	if err != nil {
		return nil, err
	}

	return args[0], nil
}

func (s *Session) SetGraphName(graphName string) error {
	if s.graphNameDefined {
		return ErrGraphNameDefinedForSession
	}
	s.mutex.Lock()
	s.GraphName = graphName
	s.mutex.Unlock()
	return nil
}

func (s *Session) SetGraphObjName(objName string) error {
	if s.graphObjNameDefined {
		return ErrGraphNameDefinedForSession
	}
	s.mutex.Lock()
	s.GraphObjName = objName
	s.mutex.Unlock()
	return nil
}
