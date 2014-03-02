// Copyright (c) 2013 LinkOmnia Limited. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file.

/*
Package rexgo is a Rexster client for Go using the binary RexPro protocol.
The actual RexPro version 1 wire protocol is implemented in the package
"net/rexpro1".

Note that only RexPro version 1 is implemented at this time. This means Rexster
versions older than 2.4.0 are not supported.

This package uses the excellent codec from ugorji for MsgPack serialization:
https://github.com/ugorji/go/tree/master/codec

For more information about the Rexster graph database and the RexPro protocol,
see: https://github.com/tinkerpop/rexster/wiki
*/
package rexgo

import (
	"errors"
	"github.com/linkomnia/rexgo/net/rexpro1"
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
	rx           *rexpro1.Client
	mutex        sync.Mutex
}

// Dial connects to a Rexster server at the specified network address.
func Dial(addr string) (*Client, error) {
	rx, err := rexpro1.Dial(addr)
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

func (client *Client) newMetaMap() rexpro1.MetaMap {
	meta := make(rexpro1.MetaMap)

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
	args := []interface{}{username, password}

	sid, _, _, _, err := client.rx.Call(rexpro1.EmptySession, rexpro1.MsgSessionRequest, meta, args)
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
	args := []interface{}{"", ""}

	s.mutex.Lock()
	sid := s.Id
	s.mutex.Unlock()
	_, _, _, _, err := s.client.rx.Call(sid, rexpro1.MsgSessionRequest, meta, args)
	return err
}

func decodeVerticesAndEdges(results []interface{}) {
	for i, o := range results {
		switch o := o.(type) {
		case map[string]interface{}:
			switch o["_type"] {
			case "vertex":
				if v := toVertex(o); v != nil {
					results[i] = v
				}
			case "edge":
				if e := toEdge(o); e != nil {
					results[i] = e
				}
			}
		case []interface{}:
			decodeVerticesAndEdges(o)
		}
	}
}

// Execute sends the Gremlin script to the Rexster server, waits for it to complete, and returns the results.
//
// Example:
//  bindings := map[string]interface{}{"godname": "saturn"}
//  result, err := rx.Script(`g.V('name',godname).in('father').in('father').name`, bindings)
//  if result[0] != "hercules" {
//      // uh-oh
//  }
func (client *Client) Execute(script string, bindings map[string]interface{}) (results []interface{}, err error) {
	meta := client.newMetaMap()
	args := []interface{}{"groovy", script, bindings}

	_, _, _, args, err = client.rx.Call(rexpro1.EmptySession, rexpro1.MsgScriptRequest, meta, args)
	if err != nil {
		return nil, err
	}
	if len(args) < 1 {
		return nil, nil
	}

	results, ok := args[0].([]interface{})
	if !ok {
		results = make([]interface{}, 1)
		results[0] = args[0]
	}
	decodeVerticesAndEdges(results)
	return results, nil
}

func (s *Session) newMetaMap() rexpro1.MetaMap {
	meta := make(rexpro1.MetaMap)

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

// Execute sends the Gremlin script to the Rexster server, waits for it to
// complete, and returns the results. Variable bindings are preserved in the
// same session.
func (s *Session) Execute(script string, bindings map[string]interface{}) (results []interface{}, err error) {
	meta := s.newMetaMap()
	args := []interface{}{"groovy", script, bindings}

	s.mutex.Lock()
	sid := s.Id
	s.mutex.Unlock()
	_, _, _, args, err = s.client.rx.Call(sid, rexpro1.MsgScriptRequest, meta, args)
	if err != nil {
		return nil, err
	}
	if len(args) < 1 {
		return nil, nil
	}

	results, ok := args[0].([]interface{})
	if !ok {
		results = make([]interface{}, 1)
		results[0] = args[0]
	}
	decodeVerticesAndEdges(results)
	return results, nil
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

// Vertex represents a vertex object as returned by Gremlin.
type Vertex struct {
	Id   string
	Prop map[string]interface{}
}

func toVertex(vmap map[string]interface{}) *Vertex {
	vid, ok := vmap["_id"].(string)
	if !ok {
		return nil
	}
	vprop, ok := vmap["_properties"].(map[string]interface{})
	if !ok {
		return nil
	}
	return &Vertex{vid, vprop}
}

// Edge represents an edge object as returned by Gremlin.
type Edge struct {
	Id          string
	InVertexID  string
	OutVertexID string
	Label       string                 // can be empty
	Prop        map[string]interface{} // can be nil
}

func toEdge(emap map[string]interface{}) *Edge {
	eid, ok := emap["_id"].(string)
	if !ok {
		return nil
	}
	inv, ok := emap["_inV"].(string)
	if !ok {
		return nil
	}
	outv, ok := emap["_outV"].(string)
	if !ok {
		return nil
	}
	label, _ := emap["_label"].(string)
	eprop, _ := emap["_properties"].(map[string]interface{})
	return &Edge{eid, inv, outv, label, eprop}
}
