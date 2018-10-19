package model

import (
	"fmt"
	"testing"
)

func countAndSay(n int) string {
	if n <= 0 {
		return ""
	}
	if n == 1 {
		return "1"
	}
	last := []byte(countAndSay(n - 1))
	out := ""
	count := 1
	d := last[0]
	for i := 1; i <= len(last); i++ {
		if i < len(last) {
			if last[i] == d {
				count++
			} else {
				out += fmt.Sprintf("%d%s", count, string(d))
				d = last[i]
				count = 1
			}

		} else {
			out += fmt.Sprintf("%d%s", count, string(d))
		}
	}
	return out
}

func TestCountAndSay(t *testing.T) {

	result := countAndSay(4)
	fmt.Println(result)
}

func magicalString(n int) int {
	magic := make([]int, n+1)
	digit := []int{1, 2}
	magic[0] = 1
	magic[1] = 2

	d := 1
	for g := 1; d < n; g++ {
		c := digit[g%2]
		r := magic[g]
		if r == 1 {
			magic[d] = c
			d++
		} else {
			magic[d] = c
			magic[d+1] = c
			d += 2
		}
	}

	ones := 0
	for i, d := range magic {
		if d == 1 && i < n {
			ones++
		}
	}

	return ones
}

func TestMagicalString(t *testing.T) {

	magicalString(1)
	magicalString(6)
}
