package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Leader struct {
	node string
	root string
	conn *zk.Conn
	sid  string
	stop chan bool
}

func main() {
	ld, err := NewLeaderElecter([]string{"127.0.0.1:2181"}, "/clc", "master", time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		ld.Close()
	}()

	go func() {
		for {
			isLeader, err := ld.GrabLeader()
			if err != nil {
				fmt.Println("isleader:", err)
			}

			if isLeader == false {
				fmt.Println("not leader")
			} else {
				fmt.Println("is leader")
				for {
					time.Sleep(time.Second * 1000)
				}
			}
		}
	}()

	// wait signal
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGUSR1)
	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGUSR1:
			continue
		}
		break
	}
}

func NewLeaderElecter(host []string, root, node string, timeout time.Duration) (*Leader, error) {
	c, _, err := zk.Connect(host, timeout)
	if err != nil {
		fmt.Println("connect zookeeper failed.")
		return nil, err
	}

	ex, _, err := c.Exists(root)
	if err == nil && ex == false {
		//if _, e := c.Create(root, nil, 0, zk.WorldACL(zk.PermAll)); e != nil {
		if _, e := c.Create(root, []byte("1"), 0, zk.WorldACL(zk.PermAll)); e != nil {
			fmt.Println("creare zookeeper node failed.")
			return nil, e
		}
	}

	path := root + "/" + node
	s, err := c.CreateProtectedEphemeralSequential(path, []byte("1"), zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println("create zookeeper node failed.")
		return nil, err
	}
	fmt.Println(s)
	fmt.Println(strings.TrimPrefix(s, root+"/"))

	return &Leader{node: node, root: root, conn: c, sid: strings.TrimPrefix(s, root+"/"), stop: make(chan bool, 1)}, nil
}

func (le *Leader) GrabLeader() (bool, error) {
	for {
		isLead, err, ch := le.TryGrabLeader()
		if err != nil {
			return false, err
		}

		if isLead {
			return true, nil
		}

		select {
		case <-le.stop:
			return false, nil
		case <-ch:
			break
		}
	}
}
func (le *Leader) TryGrabLeader() (bool, error, <-chan zk.Event) {
	child, _, ch, err := le.conn.ChildrenW(le.root)
	if err != nil {
		fmt.Println("get zookeeper node children failed.")
		return false, err, nil
	}

	if le.isLeader(child) {
		return true, nil, ch
	} else {
		return false, nil, ch
	}
}

func (le *Leader) isLeader(children []string) bool {
	if len(children) == 1 {
		return true
	}

	fmt.Println("children:", children, "node:", le.node)
	var node, sid string
	for _, s := range children {
		ids := strings.Split(s, le.node)
		id := ids[len(ids)-1]
		fmt.Println("ids:", ids, " id:", id, " node:", le.node)
		if node == "" || node > id {
			node = id
			sid = s
		}
	}

	fmt.Println(le.sid, sid)
	if sid == le.sid {
		return true
	} else {
		return false
	}
}

func (le *Leader) Close() {
	le.stop <- true
	le.conn.Close()
	fmt.Println("close")
}
