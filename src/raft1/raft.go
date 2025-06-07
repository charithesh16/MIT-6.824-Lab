package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const HEATBEAT float64 = 50    // leader send heatbit per 50ms (reduced from 150ms for faster log replication)
const TIMEOUTLOW float64 = 500 // the timeout period randomize between 500ms - 1000ms
const TIMEOUTHIGH float64 = 1000
const CHECKPERIOD float64 = 300       // check timeout per 300ms
const CHECKAPPLYPERIOD float64 = 10   // check apply per 10ms
const CHECKAPPENDPERIOED float64 = 10 // check append per 10ms
const CHECKCOMMITPERIOED float64 = 10 // check commit per 10ms
const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile
	commitIndex   int
	lastApplied   int
	lastHeartbeat time.Time
	state         int // 0: follower, 1: candidate, 2: leader
	nextIndex     []int
	matchIndex    []int

	cond    *sync.Cond
	applyCh chan raftapi.ApplyMsg
}

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Optimization for nextIndex backup
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// error occurred during decoding
		DPrintf("Error: failed to decode persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist() // persist state change
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastlogterm := rf.log[len(rf.log)-1].Term
		// the candidate's is at least as up-to-date as receiver's log, grant vote !!
		if args.LastLogTerm > lastlogterm ||
			(args.LastLogTerm == lastlogterm && args.LastLogIndex >= len(rf.log)-1) {
			// reset timer only when you **grant** the vote for another server
			rf.lastHeartbeat = time.Now()
			rf.votedFor = args.CandidateId
			rf.persist() // persist state change
			reply.VoteGranted = true
			DPrintf("[term %d]: Raft [%d] vote for Raft [%d]\n", rf.currentTerm, rf.me, rf.votedFor)
			return
		}
	}
	// meaning alreay voted for other server
	reply.VoteGranted = false
	return

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, Entry{Term: term, Command: command})
	rf.persist() // persist log change

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		r := rand.New(rand.NewSource(int64(rf.me)))
		randomTimeout := int(r.Float64()*(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW)
		rf.mu.Lock()
		if rf.state != LEADER && time.Since(rf.lastHeartbeat) > time.Duration(randomTimeout)*time.Millisecond {
			go rf.startElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// term := args.Term
	// leaderId := args.LeaderId
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	// entries := args.Entries
	// leaderCommitIndex := args.LeaderCommit

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If current raft term is greater than candidate term, then reject the request.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist() // persist state change
	}
	rf.lastHeartbeat = time.Now()

	// If the previous log index is out of bounds, then reject the request.
	if len(rf.log) <= prevLogIndex || rf.log[prevLogIndex].Term != prevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		// Optimization: set fields for faster nextIndex backup
		if len(rf.log) <= prevLogIndex {
			// Case 3: follower's log is too short
			reply.XLen = len(rf.log)
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			// Case 1 or 2: conflicting term
			reply.XTerm = rf.log[prevLogIndex].Term
			reply.XLen = len(rf.log)

			// Find index of first entry with XTerm
			reply.XIndex = prevLogIndex
			for reply.XIndex > 0 && rf.log[reply.XIndex-1].Term == reply.XTerm {
				reply.XIndex--
			}
		}
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	// Handle log entries according to Raft paper specification
	// If there are entries to append
	logChanged := false
	if len(args.Entries) > 0 {
		// Find first conflicting entry (if any)
		conflictIndex := -1
		for i, entry := range args.Entries {
			logIndex := args.PrevLogIndex + 1 + i
			if logIndex >= len(rf.log) {
				// No more entries in our log, append remaining
				break
			}
			if rf.log[logIndex].Term != entry.Term {
				// Found conflict, truncate log from this point
				conflictIndex = logIndex
				break
			}
		}

		// If we found a conflict, truncate the log
		if conflictIndex != -1 {
			rf.log = rf.log[:conflictIndex]
			logChanged = true
		}

		// Append new entries that aren't already in the log
		for i, entry := range args.Entries {
			logIndex := args.PrevLogIndex + 1 + i
			if logIndex >= len(rf.log) {
				rf.log = append(rf.log, entry)
				logChanged = true
			}
		}
	}

	// Persist if log changed
	if logChanged {
		rf.persist()
	}

	// Update commit index if leader's commit index is higher
	if args.LeaderCommit > rf.commitIndex {
		// set commitIndex = min(leaderCommit, index of last new entry)
		oriCommitIndex := rf.commitIndex
		lastNewIndex := len(rf.log) - 1
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewIndex)))
		DPrintf("[term %d]:Raft [%d] [state %d] commitIndex is %d", rf.currentTerm, rf.me, rf.state, rf.commitIndex)
		if rf.commitIndex > oriCommitIndex {
			// wake up sleeping applyCommit Go routine
			rf.cond.Broadcast()
		}
	}
	return
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) callAppendEntries(peer int, term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []Entry, leaderCommitIndex int) (bool, *AppendEntriesReply) {
	DPrintf("[term %d]:Raft [%d] [state %d] sends appendentries RPC to server[%d]\n", rf.currentTerm, rf.me, rf.state, peer)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommitIndex,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return false, nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// *** to avoid term confusion !!! ***
	// compare the current term with the term you sent in your original RPC.
	// If the two are different, drop the reply and return
	if term != rf.currentTerm {
		return false, nil
	}

	// other server has higher term !
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist() // persist state change
	}
	return reply.Success, &reply
}

// sends append entries without entries to all peers
func (rf *Raft) sendLeaderHeartBeats() {
	// send indefinitely until the current raft is leader or killed.
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		// else send heartbeats to all peers.
		term := rf.currentTerm
		leaderId := rf.me
		prevLogIndex := len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		entries := make([]Entry, 0)
		leaderCommitIndex := rf.commitIndex
		rf.mu.Unlock()
		for peer := range rf.peers {
			// if me continue
			if peer == rf.me {
				continue
			}
			// send heartbeat
			go func(peer int) {
				rf.callAppendEntries(peer, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommitIndex)
			}(peer)
		}
		time.Sleep(time.Millisecond * time.Duration(HEATBEAT))
	}
}

func (rf *Raft) allocateAppendCheckers() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendChecker(i)
	}
}

// periodically check if last log index >= nextIndex[server], if so, send AppendEntries (leader only)
func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		lastlogidx := len(rf.log) - 1
		nextidx := rf.nextIndex[server]
		term := rf.currentTerm
		preLogIndex := nextidx - 1
		preLogTerm := rf.log[preLogIndex].Term
		leaderCommit := rf.commitIndex
		entries := rf.log[nextidx:]
		rf.mu.Unlock()
		if lastlogidx >= nextidx {
			DPrintf("[term %d]: Raft[%d] send real appendEntries to Raft[%d]", rf.currentTerm, rf.me, server)
			success, reply := rf.callAppendEntries(server, term, rf.me, preLogIndex, preLogTerm, entries, leaderCommit)

			rf.mu.Lock()
			// term confusion
			if term != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			// append entries successfully, update nextIndex and matchIndex
			if success {
				rf.nextIndex[server] = nextidx + len(entries)
				rf.matchIndex[server] = preLogIndex + len(entries)
				DPrintf("[term %d]: Raft[%d] successfully append entries to Raft[%d]", rf.currentTerm, rf.me, server)
				rf.mu.Unlock()
			} else if reply != nil {
				// Use optimization for faster nextIndex backup
				if reply.XTerm == -1 {
					// Case 3: follower's log is too short
					rf.nextIndex[server] = reply.XLen
				} else {
					// Case 1 or 2: leader doesn't have XTerm or has XTerm
					found := false
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.XTerm {
							// Case 2: leader has XTerm
							rf.nextIndex[server] = i + 1
							found = true
							break
						}
					}
					if !found {
						// Case 1: leader doesn't have XTerm
						rf.nextIndex[server] = reply.XIndex
					}
				}
				// Ensure nextIndex doesn't go below 1
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				rf.mu.Unlock()
				continue
			} else {
				// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
				rf.mu.Unlock()
				continue
			}
		}
		time.Sleep(time.Millisecond * time.Duration(CHECKAPPENDPERIOED))
	}
}

func (rf *Raft) callRequestVote(server int, term int, lastlogidx int, lastlogterm int) bool {
	DPrintf("[term %d]:Raft [%d][state %d] sends requestvote RPC to server[%d]\n", term, rf.me, rf.state, server)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastlogidx,
		LastLogTerm:  lastlogterm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// *** to avoid term confusion !!! ***
	// compare the current term with the term you sent in your original RPC.
	// If the two are different, drop the reply and return
	if term != rf.currentTerm {
		return false
	}

	// other server has higher term !
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist() // persist state change
	}
	return reply.VoteGranted
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.persist() // persist state change
	//reset to current time
	rf.lastHeartbeat = time.Now()
	term := rf.currentTerm // save for RPC call
	lastlogidx := len(rf.log) - 1
	lastlogterm := rf.log[lastlogidx].Term
	currentServer := rf.me
	rf.mu.Unlock()
	DPrintf("[%d] Starting Election for Raft [%d] for term %d\n", rf.me, rf.me, rf.currentTerm)
	grantedVotes := 1
	electionFinished := false
	var voteMutex sync.Mutex
	for peer := range rf.peers {
		if peer == currentServer {
			DPrintf(" voting for self %d", currentServer)
			continue
		}
		go func(peer int) {

			voteGranted := rf.callRequestVote(peer, term, lastlogidx, lastlogterm) // returns true if we get a response if packet lost then false.
			voteMutex.Lock()
			if voteGranted && !electionFinished {
				grantedVotes++
				// then it is a leader
				if 2*grantedVotes > len(rf.peers) {
					electionFinished = true
					rf.mu.Lock()
					rf.state = LEADER
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) // rf.log indexed from 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.sendLeaderHeartBeats()
					go rf.allocateAppendCheckers()
					go rf.commitChecker()
				}
			}
			voteMutex.Unlock()
		}(peer)
	}
}

// periodically check if there exists a log entry which is commited
func (rf *Raft) commitChecker() {
	for {
		rf.mu.Lock()
		// Exit if no longer a leader or killed
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		// Find the highest index that can be committed from current term
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			if rf.log[N].Term == rf.currentTerm {
				consensus := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						consensus++
					}
				}
				if consensus*2 > len(rf.peers) {
					DPrintf("[term %d]:Raft [%d] [state %d] commit log entry %d successfully", rf.currentTerm, rf.me, rf.state, N)
					rf.commitIndex = N
					// kick the applyCommit go routine
					rf.cond.Broadcast()
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(CHECKCOMMITPERIOED))
	}
}

func (rf *Raft) applyCommited() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		DPrintf("[log]: %v", rf.log)
		cmtidx := rf.lastApplied
		command := rf.log[cmtidx].Command
		rf.mu.Unlock()
		// commit the log entry
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: cmtidx,
		}
		// this line may be blocked
		rf.applyCh <- msg
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service successfully", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1 // did not vote for anyone
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.lastHeartbeat = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommited()

	return rf
}
