/*
   Copyright 2014 David Terei.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package raft

// Blade GC Support

import (
	"log"
	"runtime"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/code.google.com/p/go.net/context"
	pb "github.com/coreos/etcd/raft/raftpb"
)

const (
	GC_BLADE_TIMEOUT = 10 * time.Second
)

// singleton, global instance
var gcm *GCManager

type GCMsg struct {
	gcID    uint64
	timeout bool
}

type GCManager struct {
	id       uint64
	minHeap  uint64
	minPause uint64
	gcnum    uint64
	gcdone   uint64
	n        Node
	gcStart  chan GCMsg
	gcReq    chan pb.Message
}

// SetupBlade setups the Blade GC handler
func SetupBlade(nodeID uint64, heapMin, pauseMin uint64, n Node) {
	if gcm != nil {
		panic("SetupBlade called twice!")
	}
	gcm = &GCManager {
		id:       nodeID,
		minHeap:  heapMin,
		minPause: pauseMin,
		n:        n,
		gcStart:  make(chan GCMsg),
		gcReq:    make(chan pb.Message),
	}
	go gcManager()
	go raftBladeManager()
	runtime.RegisterGCCallback(gcHandler)
	log.Printf("blade: setup finished [pr: %d]", nodeID)
}

// gcHandler handles a GC request from the RTS
func gcHandler(alloc, lastPause int64, ret *int64) {
	gcm.gcnum++
	if uint64(lastPause) < gcm.minPause && uint64(alloc) < gcm.minHeap {
		log.Printf("blade: ignoring gc [#: %d, alloc: %d, pause: %d]",
			gcm.gcnum, alloc, lastPause)
		*ret = 0
	} else {
		log.Printf("blade: starting gc [#: %d, alloc: %d, pause: %d]",
			gcm.gcnum, alloc, lastPause)
		*ret = 1
		go bladeGC(gcm.gcnum)
	}
}

// bladeGC runs a garbage collection using blade.
func bladeGC(gcID uint64) {
	time.AfterFunc(GC_BLADE_TIMEOUT, func() {
		gcm.gcStart <- GCMsg{gcID, true}
	})
	ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
	gcm.n.Step(ctx, pb.Message{Type: pb.MsgGCReq, Index: gcID, From: gcm.id})
	cancel()
}

// gcManager runs the collector after it was paused waiting for Blade to
// respond.
func gcManager() {
	id := gcm.id
	for {
		m := <-gcm.gcStart
		if m.gcID <= gcm.gcdone {
			continue
		}
		gcm.gcdone = m.gcID
		if (m.timeout) {
			log.Printf("blade: *** gc timeout [gc: %d] ***", m.gcID)
		} else {
			log.Printf("blade: gc authorized [gc: %d]", m.gcID)
		}
		runtime.GCStart()
		log.Printf("blade: gc finished [gc: %d]", m.gcID)

		ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
		gcm.n.Step(ctx, pb.Message{Type: pb.MsgGCDone, Index: m.gcID, From: id})
		cancel()
	}
}

// raftBladeManager manages scheduling GC for the cluster.
func raftBladeManager() {
	send := func(m pb.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
		gcm.n.Step(ctx, m)
		cancel()
	}

	me := gcm.id

	for {
		select {
		case m := <- gcm.gcReq:
			switch m.Type {
			case pb.MsgGCAllowed: fallthrough
			case pb.MsgGCAuth:
				log.Panicf("blade manager: shouldn't receive this message: %v", m)

			case pb.MsgGCReq:
				if m.From == me {
					// I want to collect and am leader...
					log.Printf("blade manager: leader self-colecting [gc: %d]", m.From)
					gcm.gcStart <- GCMsg{m.Index, false}
				} else {
					log.Printf("blade manager: gc request [pr: %d, gc: %d]",
						m.From, m.Index)
					// no policy, allow when requested
					m.Type = pb.MsgGCAuth
					m.To = m.From
					go send(m)
				}
			case pb.MsgGCDone:
				// done!
			}
		}
	}
}

