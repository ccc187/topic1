package model

import "testing"

func Test_GetKeyForTopicsBitMap(t *testing.T) {
	var key string
	var offset int64

	key, offset = GetKeyForTopicsBitMap(1)               // 1位，预期：key.part=100, offset=1
	key, offset = GetKeyForTopicsBitMap(12345678)        // 8位，预期：key.part=100, offset=12345678
	key, offset = GetKeyForTopicsBitMap(123456789)       // 9位，预期：key.part=1, offset=23456789
	key, offset = GetKeyForTopicsBitMap(1234567899)      // 10位，预期：key.part=12, offset=34567899
	key, offset = GetKeyForTopicsBitMap(123456789999999) // 15位，预期：key.part=67, offset=89999999

	println(key, offset)
}
