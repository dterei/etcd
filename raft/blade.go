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

type GCLeader struct {
	newLeader uint64
	oldLeader uint64
}

type GCManager struct {
	id       uint64
	minHeap  uint64
	minPause uint64
	n        Node
	gcnum    uint64

	gcRun    chan GCMsg
	gcLocal  chan uint64
	gcLeader chan GCLeader

	gcReq    chan pb.Message
	gcSlots  chan int
	gcReset  chan GCLeader
}

// InitBladeGC setups the Blade GC handler. Call first, then call
// 'StartBladeGC'.
func InitBladeGC(heapMin, pauseMin uint64) {
	if gcm != nil {
		panic("InitBladeGC called twice!")
	}
	gcm = &GCManager {
		minHeap:  heapMin,
		minPause: pauseMin,

		gcRun:    make(chan GCMsg),
		gcLocal:  make(chan uint64),
		// XXX: critical to buffer to prevent deadlock with raft goroutine
		gcLeader: make(chan GCLeader, 5),

		gcReq:    make(chan pb.Message),
		gcSlots:  make(chan int),
		gcReset:  make(chan GCLeader),
	}
}

// StartBladeGC starts blade running. Call after you call 'InitBladeGC'.
func StartBladeGC(nodeID uint64, n Node) {
	if gcm.n != nil {
		panic("StartBladeGC called twice!")
	}
	gcm.id = nodeID
	gcm.n = n
	go bladeNodeManager()
	go bladeClusterManager()
	runtime.RegisterGCCallback(gcCallback)
}

// gcCallback handles a GC request from the RTS
func gcCallback(alloc, lastPause int64, ret *int64) {
	gcm.gcnum++
	if uint64(lastPause) < gcm.minPause && uint64(alloc) < gcm.minHeap {
		// log.Printf("blade: ignoring gc [#: %d, alloc: %d, pause: %d]",
		// 	gcm.gcnum, alloc, lastPause)
		*ret = 0
	} else {
		log.Printf("blade: starting gc [#: %d, alloc: %d, pause: %d]",
			gcm.gcnum, alloc, lastPause)
		*ret = 1
		go func() {
			gcm.gcLocal <- gcm.gcnum
			requestGC(gcm.gcnum)
		}()
	}
}

// requestGC starts a new blade GC.
func requestGC(gc uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
	err := gcm.n.Step(ctx, pb.Message{Type: pb.MsgGCReq, Index: gc, From: gcm.id})
	cancel()
	if err != nil { gcm.gcRun <- GCMsg{gc, true} }
}

// bladeNodeManager runs the collector after it was paused waiting for Blade to
// respond.
func bladeNodeManager() {
	id := gcm.id
	reqInFlight := None
	gcComplete := None

	for {
		select {
		// new gc request
		case reqInFlight = <-gcm.gcLocal:

		// gc authorized
		case m := <-gcm.gcRun:
			if m.gcID <= gcComplete { continue }
			reqInFlight = None
			gcComplete = m.gcID

			if (m.timeout) {
				log.Printf("blade: *** gc timeout [gc: %d] ***", m.gcID)
			} else {
				log.Printf("blade: gc authorized [gc: %d]", m.gcID)
			}
			runtime.GCStart()
			log.Printf("blade: gc finished [gc: %d]", m.gcID)

			// XXX: is their a possible timing deadlock where we try to send the raft
			// goroutine this message as it is trying to send one to us?
			ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
			gcm.n.Step(ctx, pb.Message{Type: pb.MsgGCDone, Index: m.gcID, From: id})
			cancel()

		// leader changed
		case m := <-gcm.gcLeader:
			gcm.gcReset <- m
			if reqInFlight != None && m.newLeader != None {
				log.Printf("blade: resending gc [gc: %d]", reqInFlight)
				// we run in a goroutine in case of a rare timing occurence where the
				// raft goroutine is trying to send us a gc authorized message as we try
				// to send it a gc request message.
				go requestGC(reqInFlight)
			}
		}
	}
}

// bladeClusterManager manages scheduling GC for the cluster.
func bladeClusterManager() {
	send := func(m pb.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
		gcm.n.Step(ctx, m)
		cancel()
	}

	me := gcm.id
	slots := 0
	used := 0

	for {
		select {
		// change in number of gc slots
		case slots = <- gcm.gcSlots:
			_ = slots

		// leader changed
		case _ = <- gcm.gcReset:
			used = 0

		// blade gc message arrived
		case m := <- gcm.gcReq:
			switch m.Type {
			case pb.MsgGCAllowed: fallthrough
			case pb.MsgGCAuth:
				log.Panicf("blade manager: shouldn't receive this message: %v", m)

			case pb.MsgGCReq:
				if m.From == me {
					// I want to collect and am leader...
					log.Printf("blade manager: leader self-colecting [gc: %d]", m.From)
					gcm.gcRun <- GCMsg{m.Index, false}
				} else {
					log.Printf("blade manager: gc request [pr: %x, gc: %d]",
						m.From, m.Index)
					// no policy, allow when requested
					m.Type = pb.MsgGCAuth
					m.To = m.From
					go send(m)
					used++
				}

			case pb.MsgGCDone:
				used--
				log.Printf("blade manager: gc finished [pr: %x, gc: %d, used: %d]",
					m.From, m.Index, used)
			}
		}
	}
}

