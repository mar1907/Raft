import rpyc
from repeat_timer import RepeatingTimer
import random
from enum import Enum
import _thread
import time


class ServerState(Enum):
    leader = 1
    follower = 2
    candidate = 3


class RaftServer(rpyc.Service):

    # initiator function, sets default attributes
    def on_connect(self, conn):
        print("on_connect")
        self.currentTerm = 0
        self.votedFor = 0
        self.log = [(0, "")]  # list of logs of type [(t, "x=5")], where t = term and "string" = command
        self.state = {}  # dict recording state
        self.leaderIs = 0
        # set to 0 (first log entry is null, already "applied")
        self.commitIndex = 0
        self.lastApplied = 0

        self.raftState = ServerState.follower
        self.votes = 0
        pass

    # called first thing after creation, sets non-default attributes and prompts timer start
    def exposed_setAttributes(self, **kwargs):
        print(kwargs['id'], "set attributes")
        self.totalServers = kwargs['totalServers']  # total nr of servers
        self.id = kwargs['id']
        self.link = rpyc.connect('localhost', 8080, config={'allow_public_attrs': True, "allow_all_attrs": True})

        self.nextIndex = [0] * self.totalServers
        self.matchIndex = [0] * self.totalServers

        timer = random.randint(5, 10)
        self.electionTimer = RepeatingTimer(timer, self.electionTimeout)  # 150 - 500ms
        self.electionTimer.start()
        timer = 2  # random.randint(1, 2)
        self.heartbeatTimer = RepeatingTimer(timer, self.heartbeatTimeout)

        return

    def exposed_AppendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        print(self.id, "append entries from " + str(leaderId))

        self.electionTimer.cancel()
        self.electionTimer.start()

        # if term < currentTerm => False
        if term < self.currentTerm:
            print(self.id, "term < currentTerm", (False, self.currentTerm))
            return False, self.currentTerm

        self.currentTerm = term

        # if log doesn't contain an entry at prevLogIndex => False
        if len(self.log) - 1 < prevLogIndex:
            print(self.id, "log doesn't contain an entry at prevLogIndex")
            return False, self.currentTerm

        # if log contains an entry at prevLogIndex that doesn't match prevLogTerm => False
        if self.log[prevLogIndex][0] != prevLogTerm:
            print(self.id, "log contains an entry at prevLogIndex that doesn't match prevLogTerm")
            return False, self.currentTerm

        entries = list(entries)
        if len(entries) == 0:
            # empty AppendEntries used to establish leader or as heartbeat
            print(self.id, "empty AppendEntries used to establish leader or as heartbeat")
            self.raftState = ServerState.follower
            self.leaderIs = leaderId

            # set commitIndex
            if leaderCommit > self.commitIndex:
                print(self.id, "set commitIndex")
                self.commitIndex = min(leaderCommit, len(self.log) - 1)
                if self.commitIndex > self.lastApplied:
                    self.applyChanges()
                    self.lastApplied = self.commitIndex

            return True, self.currentTerm

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        # Append any new entries not already in the log

        # for each entry
        for i in range(len(entries)):
            # if the entry already exists in self log
            if len(self.log) - 1 >= prevLogIndex + 1 + i:
                print(self.id, "entry already exists in self log")
                # if the entries have different terms
                if self.log[prevLogIndex + 1 + i][0] != entries[i][0]:
                    print(self.id, "the entries have different terms")
                    # delete the entry and the following entries
                    print(self.id, "delete the entry and the following entries")
                    del self.log[prevLogIndex + 1 + i:]
                # else the entry already exists and is correct
            else:
                # append the new entry
                print(self.id, "append the new entry")
                self.log.append(entries[i])

        print(self.id, "returning True, current log is ", str(self.log))
        return True, self.currentTerm

    def exposed_AppendResponse(self, partnerId, appendResponse):
        appendResponse = tuple(appendResponse)
        print(self.id, "append response from " + str(partnerId), appendResponse)

        # check if still leader, if not discard
        if self.raftState != ServerState.leader:
            return

        # if bigger term discovered => convert to follower, exit
        if appendResponse[1] > self.currentTerm:
            print(self.id, "bigger term discovered => convert to follower, exit")
            self.currentTerm = appendResponse[1]
            self.raftState = ServerState.follower
            self.heartbeatTimer.cancel()
            self.electionTimer.start()
            self.votedFor = 0
            self.leaderIs = 0
            return

        # if successful AppendEntries
        if appendResponse[0]:
            print(self.id, "successful AppendEntries")
            self.nextIndex[partnerId - 1] = len(self.log)
            self.matchIndex[partnerId - 1] = len(self.log) - 1
        else:
            print(self.id, "unsuccessful AppendEntries: decrement nextIndex and retry")
            # unsuccessful AppendEntries: decrement nextIndex and retry
            self.nextIndex[partnerId - 1] -= 1
            snd = tuple(self.log[self.nextIndex[partnerId - 1]:])
            _thread.start_new_thread(self.helper_send, (self.id, partnerId, self.currentTerm,
                                                        self.nextIndex[partnerId - 1] - 1,
                                                        self.log[self.nextIndex[partnerId - 1] - 1][0], snd,
                                                        self.commitIndex))

        # update commitIndex if necessary
        for i in range(self.commitIndex+1, len(self.log)):
            c = 0
            for mI in self.matchIndex:
                if mI >= i:
                    c += 1

            if c > self.totalServers / 2 and self.log[i][0] == self.currentTerm: # remove second condition?
                print(self.id, "update commitIndex", i)
                self.commitIndex = i

        if self.commitIndex > self.lastApplied:
            self.applyChanges()
            self.lastApplied = self.commitIndex

    def exposed_RequestVote(self, term, candId, lastLogIndex, lastLogTerm):
        print(self.id, "vote request from " + str(candId))
        if term < self.currentTerm:
            print(self.id, "returning false")
            return False, self.currentTerm

        if term > self.currentTerm:
            print(self.id, "higher term discovered")
            self.currentTerm = term
            self.votedFor = 0
            self.leaderIs = 0

            # if we were the leader
            if self.raftState == ServerState.leader:
                print(self.id, "we were leader but higher term discovered, convert to follower")
                self.heartbeatTimer.cancel()
                self.electionTimer.start()

            self.raftState = ServerState.follower

        last_log = self.log[-1]
        last_log_idx = len(self.log) - 1
        print(last_log, last_log_idx, lastLogTerm, lastLogIndex)

        if (self.votedFor == 0 or self.votedFor == candId) and (
                last_log[0] <= lastLogTerm and last_log_idx <= lastLogIndex):
            print(self.id, "returning true")
            self.votedFor = candId
            self.electionTimer.cancel()
            self.electionTimer.start()
            self.raftState = ServerState.follower
            return True, self.currentTerm

        print(self.id, "returning false")
        return False, self.currentTerm

    def exposed_VoteResponse(self, partnerId, voteResponse):
        print(self.id, "vote response from " + str(partnerId))

        # result from an old election, discard
        if self.raftState == ServerState.follower:
            return

        # if response is True
        if voteResponse[0]:
            self.votes += 1
        else:
            # response is false
            # if larger term discovered - cancel election
            if voteResponse[1] > self.currentTerm:
                print(self.id, "cancel election")
                self.currentTerm = voteResponse[1]
                self.votedFor = 0

                if self.raftState == ServerState.leader:
                    self.heartbeatTimer.cancel()
                elif self.raftState == ServerState.candidate:
                    self.electionTimer.cancel()

                self.electionTimer.start()
                self.raftState = ServerState.follower
            return

        if self.votes > self.totalServers / 2 and self.raftState != ServerState.leader:
            print(self.id, "became leader")
            print(self.id, "cancel election timer")
            self.electionTimer.cancel()
            self.raftState = ServerState.leader
            # send heartbeat, should be done in a different thread as to not block this remote procedure call
            _thread.start_new_thread(self.heartbeatTimeout, ())
            # become leader
            self.nextIndex = [len(self.log)] * self.totalServers
            self.matchIndex = [0] * self.totalServers
            self.leaderIs = self.id

    def electionTimeout(self):
        print(self.id, "election timeout")
        self.raftState = ServerState.candidate
        self.votes = 1  # vote for self
        self.votedFor = self.id
        self.currentTerm += 1
        self.leaderIs = 0

        # restart election timer
        self.electionTimer.start()

        # send RequestVote to all
        # this is an asynchronous RPC, it will not block while client works
        rpyc.async_(self.link.root.send_VoteRequest)(self.id, 0, self.currentTerm, len(self.log) - 1, self.log[-1][0])

    def heartbeatTimeout(self):
        print(self.id, "heartbeat timeout")

        # this is an asynchronous RPC, it will not block while client works
        rpyc.async_(self.link.root.send_AppendEntries)(self.id, 0, self.currentTerm, self.id, len(self.log) - 1,
                                                       self.log[-1][0], (), self.commitIndex)
        self.heartbeatTimer.start()

    # functions used to
    def exposed_Command(self, command):
        print(self.id, "command received " + command)

        if self.raftState == ServerState.follower:
            # we are not leader, redirect request to leader
            print(self.id, "we are not leader, redirect request to leader")
            rpyc.async_(self.link.root.send_Command)(self.id, self.leaderIs, command)
            pass

        if self.raftState == ServerState.leader:
            print(self.id, "we are leader, add request to log and commence appendEntries sequence")
            # we are leader, add request to log and commence appendEntries sequence
            self.log.append((self.currentTerm, command))
            self.nextIndex[self.id - 1] = len(self.log)
            self.matchIndex[self.id - 1] = len(self.log) - 1
            # send appendEntries to other processes
            last_log_index = len(self.log) - 1
            snd = tuple([self.log[-1]])
            print(snd)
            rpyc.async_(self.link.root.send_AppendEntries)(self.id, 0, self.currentTerm, self.id,
                                                           len(self.log) - 2, self.log[-2][0], snd,
                                                           self.commitIndex)

    # parse commands from log and add them to state map
    def applyChanges(self):
        print(self.id, "apply changes")
        for l in self.log[self.lastApplied+1:self.commitIndex+1]:
            print(self.id, "apply", l)
            if l[1] == "":
                continue
            s = l[1].split("=")
            k = s[0]
            v = int(s[1])
            self.state[k] = v

        print(self.id, "state is", str(self.state))

    # this function must exist to introduce the small sleep, RPyC will not reliably send the AppendEntries without it
    def helper_send(self, id, pid, ct, nid, l, snd, ci):
        time.sleep(1)
        rpyc.async_(self.link.root.send_AppendEntries)(id, pid, ct, pid, nid, l, snd, ci)
