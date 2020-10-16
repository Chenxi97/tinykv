// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage is nil")
	}
	log := &RaftLog{storage: storage}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// copy stable entries
	if firstIndex <= lastIndex {
		es, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
		log.entries = append(log.entries, es...)
	}
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled == l.LastIndex() {
		return []pb.Entry{}
	}
	return l.entries[l.stabled+1-l.FirstIndex():]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := max(l.applied+1, l.FirstIndex())
	if l.committed+1 > offset {
		var err error
		ents, err = l.slice(offset, l.committed+1)
		if err != nil {
			panic(err)
		}
	}
	return ents
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(to uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [dummyIndex, lastIndex]
	dummyIndex := l.FirstIndex() - 1
	if to < dummyIndex || to > l.LastIndex() {
		return 0, nil
	}
	// entris in range [firstIndex, lastIndex]
	if to > dummyIndex {
		return l.entries[to-l.FirstIndex()].Term, nil
	}
	// dummy entry
	t, err := l.storage.Term(to)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return t
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(prevIndex, prevTerm, committed uint64, ents ...pb.Entry) (index uint64, ok bool) {
	if l.matchTerm(prevIndex, prevTerm) {
		index = prevIndex + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with commited entry[committed(%d)]", ci, l.committed)
		default:
			offset := prevIndex + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, index))
		return index, true
	}
	return 0, false
}

// append may truncate log entires and reset stabled,
// return last index of new entries
func (l *RaftLog) append(es ...pb.Entry) uint64 {
	if len(es) == 0 {
		return l.LastIndex()
	}
	offset := es[0].Index
	if offset <= l.committed {
		log.Panicf("offset(%d) is out of range [committed(%d)]", offset, l.committed)
	}
	if offset <= l.LastIndex() {
		l.entries, _ = l.slice(l.FirstIndex(), offset)
	}
	l.entries = append(l.entries, es...)
	l.stabled = min(l.stabled, offset-1)
	return l.LastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The index of the given entries MUST be continuously increasing.
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) maybeCommit(curTerm, index uint64) bool {
	term, _ := l.Term(index)
	// leader only commits entries at current term
	if curTerm == term && index > l.committed {
		l.commitTo(index)
		return true
	}
	return false
}

func (l *RaftLog) commitTo(to uint64) {
	// never decrease
	if to > l.committed {
		if l.LastIndex() < to {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)].", to, l.LastIndex())
		}
		l.committed = to
	}
}

func (l *RaftLog) appliedTo(to uint64) {
	if l.committed < to || to < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", to, l.applied, l.committed)
	}
	l.applied = to
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	offset := l.FirstIndex()
	if lo < offset {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	return l.entries[lo-offset : hi-offset], nil
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) fellowerFastRollBack(prevIndex, prevTerm uint64) (conflictIndex, conflictTerm uint64) {
	lastIndex := l.LastIndex()
	if prevIndex > lastIndex {
		conflictTerm = 0
		conflictIndex = lastIndex + 1
	} else {
		conflictTerm = l.zeroTermOnErrCompacted(l.Term(prevIndex))
		for conflictIndex = prevIndex; conflictIndex > l.committed+1; conflictIndex-- {
			if l.zeroTermOnErrCompacted(l.Term(conflictIndex-1)) != conflictTerm {
				break
			}
		}
	}
	return conflictIndex, conflictTerm
}

func (l *RaftLog) leaderFastRollBack(conflictIndex, conflictTerm uint64) (nextIndex uint64) {
	if conflictTerm == 0 {
		nextIndex = conflictIndex
	} else {
		firstIndex := l.FirstIndex()
		for nextIndex = conflictIndex; nextIndex > firstIndex; nextIndex-- {
			if l.zeroTermOnErrCompacted(l.Term(nextIndex-1)) <= conflictTerm {
				break
			}
		}
	}
	return nextIndex
}
