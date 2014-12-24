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
	"container/list"
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
	gcID    uint32
	timeout bool
}

type GCSwitch struct {
	oldLeader uint64
	term      uint64
}

type GCLeader struct {
	newLeader uint64
	term      uint64
}

type GCManager struct {
	id       uint64
	minHeap  uint64
	minPause uint64
	n        Node

	gcRun    chan GCMsg
	gcLocal  chan uint32
	gcLeader chan GCLeader

	gcReq    chan pb.Message
	gcSlots  chan int
	gcReset  chan GCLeader
	gcSwitch chan GCSwitch
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
		gcLocal:  make(chan uint32),
		// XXX: critical to buffer to prevent deadlock with raft goroutine
		gcLeader: make(chan GCLeader, 5),

		gcReq:    make(chan pb.Message),
		gcSlots:  make(chan int),
		gcReset:  make(chan GCLeader),
		gcSwitch: make(chan GCSwitch),
	}
	go bladeNodeManager()
	go bladeClusterManager()
}

// StartBladeGC starts blade running. Call after you call 'InitBladeGC'.
func StartBladeGC(nodeID uint64, n Node) {
	if gcm.n != nil {
		panic("StartBladeGC called twice!")
	}
	gcm.id = nodeID
	gcm.n = n
	runtime.RegisterGCCallback(deferGCCallback)
}

// deferGCCallback handles a GC request from the RTS
func deferGCCallback(gcnum uint32, alloc, lastPause uint64) bool {
	if lastPause < gcm.minPause && alloc < gcm.minHeap {
		// log.Printf("blade: ignoring gc [#: %d, alloc: %d, pause: %d]",
		// 	gcm.gcnum, alloc, lastPause)
		return false
	} else {
		log.Printf("blade: starting gc [#: %d, alloc: %d, pause: %d]",
			gcnum, alloc, lastPause)
		go func() {
			gcm.gcLocal <- gcnum
			requestGC(gcnum)
		}()
		return true
	}
}

// requestGC starts a new blade GC.
func requestGC(gc uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), GC_BLADE_TIMEOUT)
	err := gcm.n.Step(ctx, pb.Message{
		Type: pb.MsgGCReq, Index: uint64(gc), From: gcm.id})
	cancel()
	if err != nil { gcm.gcRun <- GCMsg{gc, true} }
}

// bladeNodeManager runs the collector after it was paused waiting for Blade to
// respond.
func bladeNodeManager() {
	id := gcm.id
	var reqInFlight, gcComplete uint32
	lastLeader := None

	for {
		select {
		// new gc request
		case reqInFlight = <-gcm.gcLocal:

		// gc authorized
		case m := <-gcm.gcRun:
			if m.gcID <= gcComplete { continue }
			reqInFlight = 0
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
			gcm.n.Step(ctx, pb.Message{
				Type: pb.MsgGCDone, Index: uint64(m.gcID), From: id})
			cancel()

		// leader changed
		case m := <-gcm.gcLeader:
			gcm.gcReset <- m
			if m.newLeader != None {
				if reqInFlight != 0 {
					if lastLeader == gcm.id {
						// I was leader so have client requests queued, delay a moment to
						// let them get forwarded.
						gc := reqInFlight
						time.AfterFunc(500 * time.Millisecond, func() {
							log.Printf("blade: resending gc [gc: %d] (delayed)", gc)
							requestGC(gc)
						})
					} else {
						log.Printf("blade: resending gc [gc: %d]", reqInFlight)
						// we run in a goroutine in case of a rare timing occurence where the
						// raft goroutine is trying to send us a gc authorized message as we try
						// to send it a gc request message.
						go requestGC(reqInFlight)
					}
				}
				lastLeader = m.newLeader
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

	switchLeader := func(cand uint64) {
		// delay to allow cluster to catch up in case of lots of recent collections
		time.Sleep(1000 * time.Millisecond)
		log.Printf("blade manager: switching leader [pr: %d]", cand)
		gcm.n.FastSwitch(context.TODO(), cand)
		// we don't resend here as we rely on bladeNodeManager instead
		// doing the resend when it notices the leadership change!
	}

	slots := 0
	used := 0
	pending := list.New()
	lastCollected := None
	gcSwitch := GCSwitch{}

	for {
		select {
		// change in number of gc slots
		case slots = <- gcm.gcSlots:
			log.Printf("blade manager: change of slots: %d", slots)

		// old leader is explicitly switching to me so it can collect
		case gcSwitch = <- gcm.gcSwitch:
			log.Printf("blade manager: expecting to become leader: %v", gcSwitch)

		// leader changed
		case m := <- gcm.gcReset:
			pending.Init()
			used = 0
			lastCollected = None
			if m.newLeader != None && gcSwitch.term != None {
				if m.term == gcSwitch.term && m.newLeader == gcm.id {
					log.Printf("blade manager: became leader as expected")
				} else {
					log.Printf("blade manager: not leader: (%v) vs (%v) [%d]",
						gcSwitch, m, gcm.id)
					gcSwitch = GCSwitch{}
				}
			}

		// blade gc message arrived
		case m := <- gcm.gcReq:
			switch m.Type {
			case pb.MsgGCAllowed: fallthrough
			case pb.MsgGCAuth:
				log.Panicf("blade manager: shouldn't receive this message: %v", m)

			case pb.MsgGCReq:
				m.Type = pb.MsgGCAuth
				m.To = m.From
				if used < slots && (gcSwitch.oldLeader == None || gcSwitch.oldLeader == m.From) {
					if gcSwitch.oldLeader == m.From {
						gcSwitch = GCSwitch{}
					}
					if m.From == gcm.id {
						used = slots // stop anyone else collecting until leader done
						log.Printf("blade manager: leader self-colecting [gc: %d]", m.Index)
						go switchLeader(lastCollected)
					} else {
						used++
						log.Printf("blade manager: gc request [pr: %x, gc: %d]", m.From, m.Index)
						go send(m)
					}
				} else {
					log.Printf("blade manager: gc queued [pr: %x, gc: %d, used: %d, " +
						"slots: %d, switch: %v]", m.From, m.Index, used, slots, gcSwitch)
					pending.PushBack(m)
				}

			case pb.MsgGCDone:
				used--
				log.Printf("blade manager: gc finished [pr: %x, gc: %d, used: %d]",
					m.From, m.Index, used)
				if used < slots && pending.Len() > 0 {
					e := pending.Front()
					m = e.Value.(pb.Message)
					pending.Remove(e)
					log.Printf("blade manager: gc resuming [pr: %x, gc: %d]",
						m.From, m.Index)
					if m.From == gcm.id {
						used = slots // stop anyone else collecting until leader done
						log.Printf("blade manager: leader self-colecting [gc: %d]", m.Index)
						go switchLeader(lastCollected)
					} else {
						used++
						go send(m)
					}
				}
			}
		}
	}
}

