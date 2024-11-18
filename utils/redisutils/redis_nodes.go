// Created by lsne on 2022-11-06 21:52:12

package redisutils

import (
	"sort"
	"time"
)

type ClusterNode struct {
	IPPort          string
	Myself          bool
	ID              string
	IP              string
	Port            int
	Role            string
	FailStatus      []string
	MasterID        string
	LinkState       string
	PingSent        int64
	PongRecv        int64
	ConfigEpoch     int64
	Slots           []Slot
	balance         int
	MigratingSlots  map[Slot]string
	ImportingSlots  map[Slot]string
	ServerStartTime time.Time
}

// TotalSlots return the total number of slot
func (n *ClusterNode) TotalSlots() int {
	return len(n.Slots)
}

type ClusterNodes struct {
	MyNode *ClusterNode
	Nodes  map[string]*ClusterNode
}

// Nodes represent a Node slice
type Nodes []*ClusterNode

func (n *ClusterNode) Balance() int {
	return n.balance
}

func (n *ClusterNode) SetBalance(balance int) {
	n.balance = balance
}

// SortByFunc returns a new ordered NodeSlice, determined by a func defining ‘less’.
func (n Nodes) SortByFunc(less func(*ClusterNode, *ClusterNode) bool) Nodes {
	//result := make(Nodes, len(n))
	//copy(result, n)
	by(less).Sort(n)
	return n
}

// By is the type of a "less" function that defines the ordering of its Node arguments.
type by func(p1, p2 *ClusterNode) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (b by) Sort(nodes Nodes) {
	ps := &nodeSorter{
		nodes: nodes,
		by:    b, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}

// nodeSorter joins a By function and a slice of Nodes to be sorted.
type nodeSorter struct {
	nodes Nodes
	by    func(p1, p2 *ClusterNode) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *nodeSorter) Len() int {
	return len(s.nodes)
}

// Swap is part of sort.Interface.
func (s *nodeSorter) Swap(i, j int) {
	s.nodes[i], s.nodes[j] = s.nodes[j], s.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *nodeSorter) Less(i, j int) bool {
	return s.by(s.nodes[i], s.nodes[j])
}
