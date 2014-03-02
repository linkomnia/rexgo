package rexgo

import "testing"

func TestDial(t *testing.T) {
	addr := "localhost:8184"
	rx, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial(%v) returned %v", addr, err)
	}
	rx.Close()
}

func TestSession(t *testing.T) {
	addr := "localhost:8184"
	rx, err := Dial(addr)
	if err != nil {
		t.Fatalf("client.Dial(%v) returned %v", addr, err)
	}
	defer rx.Close()

	s, err := rx.NewSession()
	if err != nil {
		t.Fatalf("client.NewSession() returned %v", err)
	}
	defer s.Close()

	obj, err := s.Execute(`[name:var1]`, map[string]interface{}{"var1": "value1"})
	if err != nil {
		t.Fatalf("session.Script() returned %v", err)
	}
	if result, ok := obj[0].(map[string]interface{}); ok {
		if result["name"] != "value1" {
			t.Errorf(`script result: "name" expected to have "value1", got %v`, result["name"])
		}
	} else {
		t.Errorf(`script result expected to be a map, got: %#v`, obj)
	}

	obj, err = s.Execute(`[name2:var1]`, nil)
	if err != nil {
		t.Fatalf("session.Script() returned %v", err)
	}
	if result, ok := obj[0].(map[string]interface{}); ok {
		if result["name2"] != "value1" {
			t.Errorf(`script result: "name2" expected to have "value1", got %v`, result["name"])
		}
	} else {
		t.Errorf(`script result expected to be a map, got: %#v`, obj)
	}

	err = s.Close()
	if err != nil {
		t.Fatalf("session.Close() returned %v", err)
	}
}

func TestGraphOfGods(t *testing.T) {
	rx, err := Dial("localhost:8184")
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rx.Close()

	rx.SetGraphName("graph")

	bindings := map[string]interface{}{"godname": "saturn"}
	obj, err := rx.Execute(`g.V('name',godname).in('father').in('father').name`, bindings)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(obj) < 1 {
		t.Errorf(`no objects returned`)
		return
	}
	if obj[0] != "hercules" {
		t.Errorf(`Saturn's grandchild expected to be Hercules, got: %#v`, obj)
	}
}
