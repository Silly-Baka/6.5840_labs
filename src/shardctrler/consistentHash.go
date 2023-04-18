package shardctrler

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashRing struct {
	Ring            []int         // logical Ring，store node's key sequentially
	RealNodesMap    map[int]int   // virtualNode --> realNode
	VirtualNodesMap map[int][]int // realNode --> virtualNode
	VirtualNodeNum  int           // the number of virtualNode each real node
}

func NewHashRing(virtualNodeNum int) *HashRing {
	return &HashRing{
		Ring:           make([]int, 0),
		RealNodesMap:   make(map[int]int),
		VirtualNodeNum: virtualNodeNum,
	}
}

// create virtualNode for node, and insert them into RealNodesMap
func (hr *HashRing) addNode(node int) {
	// add virtual node
	virtualNodes, ok := hr.VirtualNodesMap[node]
	if !ok {
		virtualNodes = make([]int, 0)
	}
	for i := 0; i < hr.VirtualNodeNum; i++ {
		key := strconv.Itoa(i) + strconv.Itoa(node)
		hash := int(crc32.ChecksumIEEE([]byte(key)))

		hr.RealNodesMap[hash] = node
		hr.Ring = append(hr.Ring, hash)

		virtualNodes = append(virtualNodes, hash)
	}

	hr.VirtualNodesMap[node] = virtualNodes
	// sort the ring increasing
	sort.Ints(hr.Ring)
}

// remove the node，and reset the Ring
func (hr *HashRing) removeNode(node int) {

	virtualNodes, ok := hr.VirtualNodesMap[node]
	if !ok {
		return
	}
	// remove all the virtual node
	for _, v := range virtualNodes {

		delete(hr.RealNodesMap, v)

		// remove from the Ring
		idx := sort.Search(len(hr.Ring), func(i int) bool {
			return hr.Ring[i] >= v
		})
		if idx == len(hr.Ring) || hr.Ring[idx] != v {
			continue
		}

		preArr := append([]int{}, hr.Ring[:idx]...)

		hr.Ring = append(preArr, hr.Ring[idx:]...)
	}
	delete(hr.VirtualNodesMap, node)
}

// get the node that responsible for the shard
func (hr *HashRing) getNode(shard int) int {

	hash := int(crc32.ChecksumIEEE([]byte(strconv.Itoa(shard))))

	// check the Ring，find the node that bigger than hash
	idx := sort.Search(len(hr.Ring), func(i int) bool {
		return hr.Ring[i] >= hash
	})

	// mock the ring，return to the start
	if idx == len(hr.Ring) {
		idx = 0
	}

	// return the realNode
	return hr.RealNodesMap[hr.Ring[idx]]
}
