import rpyc
from raft_server import RaftServer
from rpyc.utils.server import ThreadedServer
import sys
import time
import _thread
import numpy as np


ts_list = []
proxy_list = []
server_nr = 3
server_list = list(range(1, server_nr+1))

rpc = np.ones((server_nr, server_nr))


class LinkService(rpyc.Service):

    def exposed_send_VoteRequest(self, src, dest, term, lastLogIndex, lastLogTerm):
        print("client send_VoteRequest from " + str(src) + " to " + str(dest))
        d_list = []
        if dest == 0:
            d_list = list(server_list)
            d_list.remove(src)
        else:
            d_list = [dest]

        print("client send vote request list:", d_list)

        for d in d_list:
            print("client send vote request from " + str(src) + " to " + str(d))
            if rpc[src-1][d-1]:
                # delay
                r = proxy_list[d-1].root.RequestVote(term, src, lastLogIndex, lastLogTerm)

                if rpc[d-1][src-1]:
                    # delay logic
                    rpyc.async_(proxy_list[src-1].root.VoteResponse)(d, r)

    def exposed_send_AppendEntries(self, src, dest, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        print("client send_AppendEntries " + str(entries) + " from " + str(src) + " to " + str(dest))
        d_list = []
        if dest == 0:
            d_list = list(server_list)
            d_list.remove(src)
        else:
            d_list = [dest]

        print("client send append entries list:", d_list)

        for d in d_list:
            print("client send append request from " + str(src) + " to " + str(d))
            entries = tuple(entries)
            if rpc[src-1][d-1]:
                # delay
                r = proxy_list[d - 1].root.AppendEntries(term, src, prevLogIndex, prevLogTerm, entries, leaderCommit)

                if rpc[d-1][src-1]:
                    # delay
                    r = tuple(r)
                    rpyc.async_(proxy_list[src - 1].root.AppendResponse)(d, r)

    def exposed_send_Command(self, src, dest, command):
        print("client send_Command \"" + command + "\" from " + str(src) + " to " + str(dest))
        d_list = []
        if dest == 0:
            d_list = list(server_list)
            d_list.remove(src)
        else:
            d_list = [dest]

        print("client send command list:", d_list)

        for d in d_list:
            print("client send command from " + str(src) + " to " + str(d))
            # add delay logic
            rpyc.async_(proxy_list[d - 1].root.Command)(command)


def start_server(t):
    t.start()


def freeze():
    print("client freeze")
    rpc.fill(0)
    for p in proxy_list:
        p.root.electionTimer.cancel()
        p.root.heartbeatTimer.cancel()


def thaw():
    print("client thaw")
    rpc.fill(1)
    for p in proxy_list:
        p.root.electionTimer.start()


def change(matrix, val, idx, dest=0):
    if dest == 0:
        dest = list(range(server_nr))
    else:
        dest = [dest]

    idx -= 1

    if val == 0:
        proxy_list[idx].root.electionTimer.cancel()
        proxy_list[idx].root.heartbeatTimer.cancel()
    else:
        proxy_list[idx].root.electionTimer.start()

    for d in dest:
        matrix[idx][d] = val
        matrix[d][idx] = val

    print(matrix)


# missing entries
def test_case_1():
    time.sleep(10)
    me_link = rpyc.connect('localhost', 8080, config={'allow_public_attrs': True, "allow_all_attrs": True})
    me_link.root.send_Command(0, 1, "x=1")
    time.sleep(1)
    me_link.root.send_Command(0, 1, "y=2")
    time.sleep(3)

    # freeze()
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)

    # stop communications to 1, send command, then restart
    change(rpc, 0, 1)
    time.sleep(10)
    me_link.root.send_Command(0, 2, "y=3")
    time.sleep(3)
    change(rpc, 1, 1)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    # stop communications to 2, send command, then restart
    change(rpc, 0, 2)
    time.sleep(10)
    me_link.root.send_Command(0, 3, "x=3")
    time.sleep(3)
    change(rpc, 1, 2)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    # stop communications to 2, send command, then restart
    change(rpc, 0, 3)
    time.sleep(10)
    me_link.root.send_Command(0, 1, "z=4")
    time.sleep(3)
    change(rpc, 1, 3)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    freeze()
    time.sleep(3)
    freeze()
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)


# extra uncommitted entries - send command to down server
# this is an example of a membership change problem - one server has a log that is absent on other servers
# because it believed it was leader, and this log is only removed when an appendEntry with a new command
# is added
def test_case_2():
    time.sleep(10)
    me_link = rpyc.connect('localhost', 8080, config={'allow_public_attrs': True, "allow_all_attrs": True})
    me_link.root.send_Command(0, 1, "x=1")
    time.sleep(1)
    me_link.root.send_Command(0, 1, "y=2")
    time.sleep(3)

    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    # stop communications to 1, send command, then restart
    change(rpc, 0, 1)
    time.sleep(10)
    me_link.root.send_Command(0, 1, "y=3")
    time.sleep(3)
    change(rpc, 1, 1)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    # send a new command, if 1 was leader while interrupted this will trigger it to remove the last command from log
    me_link.root.send_Command(0, 2, "y=4")
    time.sleep(5)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    # stop communications to 2, send command, then restart
    change(rpc, 0, 2)
    time.sleep(10)
    me_link.root.send_Command(0, 2, "x=3")
    time.sleep(3)
    change(rpc, 1, 2)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    # send a new command, if 2 was leader while interrupted this will trigger it to remove the last command from log
    me_link.root.send_Command(0, 3, "y=5")
    time.sleep(5)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    # stop communications to 2, send command, then restart
    change(rpc, 0, 3)
    time.sleep(10)
    me_link.root.send_Command(0, 3, "z=4")
    time.sleep(3)
    change(rpc, 1, 3)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    time.sleep(5)
    # send a new command, if 3 was leader while interrupted this will trigger it to remove the last command from log
    me_link.root.send_Command(0, 1, "z=5")
    time.sleep(5)
    freeze()
    time.sleep(2)
    freeze()
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)


# process has uncommitted logs from previous terms
def test_case_3():
    for p in proxy_list[1:]:
        # ensure 1 wins election
        print("cancel", p.root.id)
        p.root.electionTimer.cancel()

    time.sleep(10)

    # send some good logs
    me_link = rpyc.connect('localhost', 8080, config={'allow_public_attrs': True, "allow_all_attrs": True})
    me_link.root.send_Command(0, 1, "x=1")
    time.sleep(1)
    me_link.root.send_Command(0, 1, "y=2")
    time.sleep(3)

    # stop communication to 1, send some more logs
    change(rpc, 0, 1)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)
    me_link.root.send_Command(0, 1, "z=1")
    me_link.root.send_Command(0, 1, "t=2")
    time.sleep(10)
    change(rpc, 1, 1)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    time.sleep(5)
    me_link.root.send_Command(0, 2, "z=3")
    time.sleep(5)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.state)
    for p in proxy_list:
        print(p.root.id, "state is", p.root.log)

    freeze()
    time.sleep(3)
    freeze()


if __name__ == "__main__":
    me = ThreadedServer(LinkService, port=8080)
    _thread.start_new_thread(start_server, (me,))
    # server activation loop
    for i in range(1, server_nr + 1):
        t = ThreadedServer(RaftServer, port=8080 + i, protocol_config={"allow_all_attrs": True})
        _thread.start_new_thread(start_server, (t,))
        ts_list.append(t)

        proxy = rpyc.connect('localhost', 8080 + i, config={'allow_public_attrs': True, "allow_all_attrs": True})
        proxy_list.append(proxy)

        proxy.root.setAttributes(totalServers=server_nr, id=i)

    # test_case_1()
    test_case_2()
    # test_case_3()

    while True:
        pass
    # Idei de test:
    # - interactiv? (dam valori la procese) e greu sa strici consensul la viteza de input umana dar macar vedem
    # diseminarea mesajelor? poate comenzi pt perturbari?
    # - basic use case
    # - use case in care consensul pica din cauza ca lipsesc safety features
    # - perturbation test
