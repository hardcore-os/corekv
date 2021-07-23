package utils

// 跳表的基础实现
type Skiplist struct {
	maxLevel int
	head     *node
}
type node struct {
	num    int // 存储的int 也作为排序的score
	next   *node
	pre    *node
	levels []*node
}

func NewSkipList() *Skiplist {
	return &Skiplist{
		maxLevel: 0,
		head: &node{
			num:    -1,
			next:   nil,
			levels: make([]*node, 0, 64),
		},
	}
}

//func (this *Skiplist)randomLevel(idx int)int {
//	return rand.Intn(idx)
//}
//
//func (this *Skiplist)getNode(target int)(*node,[]*node)  {
//	pre, preLevels := this.head, make([]*node, 0, 64)
//	for idx := this.maxLevel; idx > 0; idx--{
//		if pre == nil || pre.levels[idx] == nil{
//			continue
//		}
//		if pre.levels[idx].num == target{
//			return pre.levels[idx], preLevels
//		}
//		for pre.levels[idx] != nil{
//			if pre.levels[idx].num > target{
//				break
//			}else if pre.levels[idx].num < target {
//				pre = pre.levels[idx]
//			}else {
//				return pre.levels[idx],preLevels
//			}
//		}
//		preLevels[idx] = pre
//	}
//	for pre != nil && pre.pre != nil{
//		if pre.num == target{
//			return pre,preLevels
//		}else if pre.num < target{
//			break
//		}
//		pre = pre.pre
//	}
//	return pre,preLevels
//}
//
//func (this *Skiplist) Search(target int) bool {
//	node, _ :=  this.getNode(target)
//	return  node.num != target
//}
//
//
//func (this *Skiplist) Add(num int)  {
//	preNode, levels := this.getNode(num)
//	nextNode := preNode.next
//	newNode := &node{
//		num:    num,
//		next:   nextNode,
//		pre:    preNode,
//		levels: make([]*node,0,64),
//	}
//	maxLevel := this.randomLevel(64)
//	preNode.next = newNode
//	newNode.pre = preNode
//	newNode.next = nextNode
//	nextNode.pre = newNode
//	for i := 0; i < maxLevel; i ++{
//		newNode.levels[i] = levels[i].levels[i]
//		levels[i] = newNode
//	}
//}
//
//
//func (this *Skiplist) Erase(num int) bool {
//	preNode, levels := this.getNode(num)
//	if preNode.num != num{
//		return false
//	}
//	for i := range preNode.levels{
//
//	}
//}
