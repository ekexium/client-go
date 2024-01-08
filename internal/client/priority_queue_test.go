// Copyright 2023 TiKV Authors
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

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type FakeItem struct {
	pri   int
	value int
}

func (f *FakeItem) priority() int {
	return f.pri
}

func TestPriority(t *testing.T) {
	re := require.New(t)
	pq := NewPriorityQueue()
	for i := 1; i <= 5; i++ {
		pq.Push(&FakeItem{value: i, pri: i})
	}
	re.Equal(5, pq.Len())
	arr := pq.All()
	re.Len(arr, 5)
	pq.Reset()
	re.Equal(0, pq.Len())
	for i := 1; i <= 5; i++ {
		pq.Push(&FakeItem{value: i, pri: i})
	}
	for i := pq.Len(); i > 0; i-- {
		re.Equal(i, pq.Pop().(*FakeItem).value)
	}
}
