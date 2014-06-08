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
	"container/list"
	"errors"
	"fmt"
	"github.com/linkomnia/rexgo/net/rexpro1"
	"runtime"
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

	addr string

	mutex        sync.Mutex
	freeConn     *list.List // of *rxConn
	connRequests *list.List // of connRequest
	numOpen      int
	pendingOpens int
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during client.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh chan struct{}
	closed   bool
	dep      map[finalCloser]depSet
	lastPut  map[*rxConn]string // stacktrace of last conn's put; debug only
	maxIdle  int                // zero means defaultMaxIdleConns; negative means 0
	maxOpen  int                // <= 0 means unlimited
}

type rxConn struct {
	client *Client

	sync.Mutex  // guards following
	rx          *rexpro1.Client
	closed      bool
	finalClosed bool // rx.Close has been called

	// guarded by client.mutex
	inUse      bool
	onPut      []func() // code (with client.mutex held) run when conn is next returned
	clmuClosed bool     // same as closed, but guarded by client.mutex, for connIfFree
	// This is the Element returned by client.freeConn.PushFront(conn)
	// It's used by connIfFree to remove the conn from the freeConn list.
	listElem *list.Element
}

func (rc *rxConn) releaseConn(err error) {
	rc.client.putConn(rc, err)
}

// the rc.Client's Mutex is held.
func (rc *rxConn) closeClientLocked() func() error {
	rc.Lock()
	defer rc.Unlock()
	if rc.closed {
		return func() error { return errors.New("sql: duplicate rxConn close") }
	}
	rc.closed = true
	return rc.client.removeDepLocked(rc, rc)
}

func (rc *rxConn) Close() error {
	rc.Lock()
	if rc.closed {
		rc.Unlock()
		return errors.New("sql: duplicate rxConn close")
	}
	rc.closed = true
	rc.Unlock() // not defer; removeDep finalClose calls may need to lock

	// And now updates that require holding rc.mu.Lock.
	rc.client.mutex.Lock()
	rc.clmuClosed = true
	fn := rc.client.removeDepLocked(rc, rc)
	rc.client.mutex.Unlock()
	return fn()
}

func (rc *rxConn) finalClose() error {
	rc.Lock()

	err := rc.rx.Close()
	rc.rx = nil
	rc.finalClosed = true
	rc.Unlock()

	rc.client.mutex.Lock()
	rc.client.numOpen--
	rc.client.maybeOpenNewConnections()
	rc.client.mutex.Unlock()

	return err
}

// depSet is a finalCloser's outstanding dependencies
type depSet map[interface{}]bool // set of true bools

// The finalCloser interface is used by (*Client).addDep and related
// dependency reference counting.
type finalCloser interface {
	// finalClose is called when the reference count of an object
	// goes to zero. (*Client).mutex is not held while calling it.
	finalClose() error
}

// addDep notes that x now depends on dep, and x's finalClose won't be
// called until all of x's dependencies are removed with removeDep.
func (client *Client) addDep(x finalCloser, dep interface{}) {
	//println(fmt.Sprintf("addDep(%T %p, %T %p)", x, x, dep, dep))
	client.mutex.Lock()
	defer client.mutex.Unlock()
	client.addDepLocked(x, dep)
}

func (client *Client) addDepLocked(x finalCloser, dep interface{}) {
	if client.dep == nil {
		client.dep = make(map[finalCloser]depSet)
	}
	xdep := client.dep[x]
	if xdep == nil {
		xdep = make(depSet)
		client.dep[x] = xdep
	}
	xdep[dep] = true
}

// removeDep notes that x no longer depends on dep.
// If x still has dependencies, nil is returned.
// If x no longer has any dependencies, its finalClose method will be
// called and its error value will be returned.
func (client *Client) removeDep(x finalCloser, dep interface{}) error {
	client.mutex.Lock()
	fn := client.removeDepLocked(x, dep)
	client.mutex.Unlock()
	return fn()
}

func (client *Client) removeDepLocked(x finalCloser, dep interface{}) func() error {
	xdep, ok := client.dep[x]
	if !ok {
		panic(fmt.Sprintf("unpaired removeDep: no deps for %T", x))
	}

	l0 := len(xdep)
	delete(xdep, dep)

	switch len(xdep) {
	case l0:
		// Nothing removed. Shouldn't happen.
		panic(fmt.Sprintf("unpaired removeDep: no %T dep on %T", dep, x))
	case 0:
		// No more dependencies.
		delete(client.dep, x)
		return x.finalClose
	default:
		// Dependencies remain.
		return func() error { return nil }
	}
}

// This is the size of the connectionOpener request chan (client.openerCh).
// This value should be larger than the maximum typical value
// used for client.maxOpen. If maxOpen is significantly larger than
// connectionRequestQueueSize then it is possible for ALL calls into the *Client
// to block until the connectionOpener can satify the backlog of requests.
var connectionRequestQueueSize = 1000000

func Open(addr string) (*Client, error) {
	client := &Client{
		addr:     addr,
		openerCh: make(chan struct{}, connectionRequestQueueSize),
		lastPut:  make(map[*rxConn]string),
	}
	client.freeConn = list.New()
	client.connRequests = list.New()
	go client.connectionOpener()
	return client, nil
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (client *Client) Ping() error {
	rc, err := client.conn()
	if err != nil {
		return err
	}
	client.putConn(rc, nil)
	return nil
}

// Close closes the rexster client, releasing any open resources.
func (client *Client) Close() error {
	client.mutex.Lock()
	if client.closed { // Make client.Close idempotent
		client.mutex.Unlock()
		return nil
	}
	close(client.openerCh)
	var err error
	fns := make([]func() error, 0, client.freeConn.Len())
	for client.freeConn.Front() != nil {
		rc := client.freeConn.Front().Value.(*rxConn)
		rc.listElem = nil
		fns = append(fns, rc.closeClientLocked())
		client.freeConn.Remove(client.freeConn.Front())
	}
	client.closed = true
	for client.connRequests.Front() != nil {
		req := client.connRequests.Front().Value.(connRequest)
		client.connRequests.Remove(client.connRequests.Front())
		close(req)
	}
	client.mutex.Unlock()
	for _, fn := range fns {
		err1 := fn()
		if err1 != nil {
			err = err1
		}
	}
	return err
}

const defaultMaxIdleConns = 2

func (client *Client) maxIdleConnsLocked() int {
	n := client.maxIdle
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (client *Client) SetMaxIdleConns(n int) {
	client.mutex.Lock()
	if n > 0 {
		client.maxIdle = n
	} else {
		// No idle connections.
		client.maxIdle = -1
	}
	// Make sure maxIdle doesn't exceed maxOpen
	if client.maxOpen > 0 && client.maxIdleConnsLocked() > client.maxOpen {
		client.maxIdle = client.maxOpen
	}
	var closing []*rxConn
	for client.freeConn.Len() > client.maxIdleConnsLocked() {
		rc := client.freeConn.Back().Value.(*rxConn)
		rc.listElem = nil
		client.freeConn.Remove(client.freeConn.Back())
		closing = append(closing, rc)
	}
	client.mutex.Unlock()
	for _, c := range closing {
		c.Close()
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (client *Client) SetMaxOpenConns(n int) {
	client.mutex.Lock()
	client.maxOpen = n
	if n < 0 {
		client.maxOpen = 0
	}
	syncMaxIdle := client.maxOpen > 0 && client.maxIdleConnsLocked() > client.maxOpen
	client.mutex.Unlock()
	if syncMaxIdle {
		client.SetMaxIdleConns(n)
	}
}

// Assumes client.mu is locked.
// If there are connRequests and the connection limit hasn't been reached,
// then tell the connectionOpener to open new connections.
func (client *Client) maybeOpenNewConnections() {
	numRequests := client.connRequests.Len() - client.pendingOpens
	if client.maxOpen > 0 {
		numCanOpen := client.maxOpen - (client.numOpen + client.pendingOpens)
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		client.pendingOpens++
		numRequests--
		client.openerCh <- struct{}{}
	}
}

// Runs in a seperate goroutine, opens new connections when requested.
func (client *Client) connectionOpener() {
	for _ = range client.openerCh {
		client.openNewConnection()
	}
}

// Open one new connection
func (client *Client) openNewConnection() {
	rx, err := rexpro1.Dial(client.addr)
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if client.closed {
		if err == nil {
			rx.Close()
		}
		return
	}
	client.pendingOpens--
	if err != nil {
		client.putConnClientLocked(nil, err)
		return
	}
	rc := &rxConn{
		client: client,
		rx:     rx,
	}
	if client.putConnClientLocked(rc, err) {
		client.addDepLocked(rc, rc)
		client.numOpen++
	} else {
		rx.Close()
	}
}

// connRequest represents one request for a new connection
// When there are no idle connections available, client.conn will create
// a new connRequest and put it on the client.connRequests list.
type connRequest chan<- interface{} // takes either a *rxConn or an error

var errClientClosed = errors.New("rexgo: client is closed")

// conn returns a newly-opened or cached *rxConn
func (client *Client) conn() (*rxConn, error) {
	client.mutex.Lock()
	if client.closed {
		client.mutex.Unlock()
		return nil, errClientClosed
	}

	// If client.maxOpen > 0 and the number of open connections is over the limit
	// or there are no free connection, then make a request and wait.
	if client.maxOpen > 0 && (client.numOpen >= client.maxOpen || client.freeConn.Len() == 0) {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		ch := make(chan interface{}, 1)
		req := connRequest(ch)
		client.connRequests.PushBack(req)
		client.maybeOpenNewConnections()
		client.mutex.Unlock()
		ret, ok := <-ch
		if !ok {
			return nil, errClientClosed
		}
		switch ret.(type) {
		case *rxConn:
			return ret.(*rxConn), nil
		case error:
			return nil, ret.(error)
		default:
			panic("rexgo: Unexpected type passed through connRequest.ch")
		}
	}

	if f := client.freeConn.Front(); f != nil {
		conn := f.Value.(*rxConn)
		conn.listElem = nil
		client.freeConn.Remove(f)
		conn.inUse = true
		client.mutex.Unlock()
		return conn, nil
	}

	client.mutex.Unlock()
	rx, err := rexpro1.Dial(client.addr)
	if err != nil {
		return nil, err
	}
	client.mutex.Lock()
	client.numOpen++
	rc := &rxConn{
		client: client,
		rx:     rx,
	}
	client.addDepLocked(rc, rc)
	rc.inUse = true
	client.mutex.Unlock()
	return rc, nil
}

var (
	errConnClosed = errors.New("rexgo: internal sentinel error: conn is closed")
	errConnBusy   = errors.New("rexgo: internal sentinel error: conn is busy")
)

// connIfFree returns (wanted, nil) if wanted is still a valid conn and
// isn't in use.
//
// The error is errConnClosed if the connection if the requested connection
// is invalid because it's been closed.
//
// The error is errConnBusy if the connection is in use.
func (client *Client) connIfFree(wanted *rxConn) (*rxConn, error) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if wanted.clmuClosed {
		return nil, errConnClosed
	}
	if wanted.inUse {
		return nil, errConnBusy
	}
	if wanted.listElem != nil {
		client.freeConn.Remove(wanted.listElem)
		wanted.listElem = nil
		wanted.inUse = true
		return wanted, nil
	}
	return nil, errConnBusy
}

// putConnHook is a hook for testing.
var putConnHook func(*Client, *rxConn)

// debugGetPut determines whether getConn & putConn calls' stack traces
// are returned for more verbose crashes.
const debugGetPut = false

// putConn adds a connection to the client's free pool.
// err is optionally the last error that occurred on this connection.
func (client *Client) putConn(rc *rxConn, err error) {
	client.mutex.Lock()
	if !rc.inUse {
		if debugGetPut {
			fmt.Printf("putConn(%v) DUPLICATE was: %s\n\nPREVIOUS was: %s", rc, stack(), client.lastPut[rc])
		}
		panic("sql: connection returned that was never out")
	}
	if debugGetPut {
		client.lastPut[rc] = stack()
	}
	rc.inUse = false

	for _, fn := range rc.onPut {
		fn()
	}
	rc.onPut = nil

	if err == rexpro1.ErrShutdown {
		// Don't reuse bad connections.
		// Since the conn is considered bad and is being discarded, treat it
		// as closed. Don't decrement the open count here, finalClose will
		// take care of that.
		client.maybeOpenNewConnections()
		client.mutex.Unlock()
		rc.Close()
		return
	}
	if putConnHook != nil {
		putConnHook(client, rc)
	}
	added := client.putConnClientLocked(rc, nil)
	client.mutex.Unlock()

	if !added {
		rc.Close()
	}
}

// Satisfy a connRequest or put the rxConn in the idle pool and return true
// or return false.
// putConnclientLocked will satisfy a connRequest if there is one, or it will
// return the *rxConn to the freeConn list if err != nil and the idle
// connection limit would not be reached.
// If err != nil, the value of rc is ignored.
// If err == nil, then rc must not equal nil.
// If a connRequest was fullfilled or the *rxConn was placed in the
// freeConn list, then true is returned, otherwise false is returned.
func (client *Client) putConnClientLocked(rc *rxConn, err error) bool {
	if client.connRequests.Len() > 0 {
		req := client.connRequests.Front().Value.(connRequest)
		client.connRequests.Remove(client.connRequests.Front())
		if err != nil {
			req <- err
		} else {
			rc.inUse = true
			req <- rc
		}
		return true
	} else if err == nil && !client.closed && client.maxIdleConnsLocked() > 0 && client.maxIdleConnsLocked() > client.freeConn.Len() {
		rc.listElem = client.freeConn.PushFront(rc)
		return true
	}
	return false
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
	rc                  *rxConn
	Id                  [16]byte // The UUID for the Session as assigned by the server.
	done                bool
}

// NewSession is a convenience function for opening a session when authentication is not required. See NewSessionWithAuth.
func (client *Client) NewSession() (*Session, error) {
	return client.NewSessionWithAuth("", "")
}

// NewSessionWithAuth asks the server to establish a new session. Variable bindings are preserved across Script requests within an open session.
func (client *Client) NewSessionWithAuth(username string, password string) (session *Session, err error) {
	for i := 0; i < 10; i++ {
		session, err = client.newSession(username, password)
		if err != rexpro1.ErrShutdown {
			break
		}
	}
	return
}

func (client *Client) newSession(username string, password string) (session *Session, err error) {
	rc, err := client.conn()
	if err != nil {
		return nil, err
	}
	rc.Lock()
	meta := client.newMetaMap()
	args := []interface{}{username, password}
	sid, _, _, _, err := rc.rx.Call(rexpro1.EmptySession, rexpro1.MsgSessionRequest, meta, args)
	rc.Unlock()
	if err != nil {
		client.putConn(rc, err)
		return nil, err
	}

	session = &Session{
		Id:                  sid,
		client:              client,
		rc:                  rc,
		graphNameDefined:    (meta["graphName"] != ""),
		graphObjNameDefined: (meta["graphObjName"] != ""),
	}
	return session, nil
}

var ErrSessionDone = errors.New("rexgo: Session has already been closed")

func (s *Session) grabConn() (*rxConn, error) {
	if s.done {
		return nil, ErrSessionDone
	}
	return s.rc, nil
}

func (s *Session) Close() error {
	if s.done {
		return ErrSessionDone
	}
	s.rc.Lock()
	meta := s.client.newMetaMap()
	meta["killSession"] = true
	args := []interface{}{"", ""}
	s.mutex.Lock()
	sid := s.Id
	s.mutex.Unlock()
	_, _, _, _, err := s.rc.rx.Call(sid, rexpro1.MsgSessionRequest, meta, args)
	s.rc.Unlock()
	s.done = true
	s.client.putConn(s.rc, nil)
	s.rc = nil
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
	for i := 0; i < 10; i++ {
		results, err = client.exec(script, bindings)
		if err != rexpro1.ErrShutdown {
			break
		}
	}
	return
}

func (client *Client) exec(script string, bindings map[string]interface{}) (results []interface{}, err error) {
	rc, err := client.conn()
	if err != nil {
		return nil, err
	}
	defer func() {
		client.putConn(rc, err)
	}()
	meta := client.newMetaMap()
	args := []interface{}{"groovy", script, bindings}
	return client.execConn(rc, rexpro1.EmptySession, meta, args)
}

func (client *Client) execConn(rc *rxConn, sid [16]byte, meta rexpro1.MetaMap, args []interface{}) (results []interface{}, err error) {
	rc.Lock()
	_, _, _, args, err = rc.rx.Call(sid, rexpro1.MsgScriptRequest, meta, args)
	rc.Unlock()
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
	rc, err := s.grabConn()
	if err != nil {
		return nil, err
	}
	return s.client.execConn(rc, sid, meta, args)
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

func stack() string {
	var buf [2 << 10]byte
	return string(buf[:runtime.Stack(buf[:], false)])
}
