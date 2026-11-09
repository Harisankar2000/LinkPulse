// Package shortid turns numeric IDs into short, non-sequential base62 codes.
//
// IDs come from a Postgres sequence handed out in blocks (see Allocator), so
// every API instance can mint codes without talking to the database on each
// request. Encoding a raw counter would make codes guessable (aaaaab, aaaaac...),
// so each ID is first passed through Scramble, a bijection over the code space.
package shortid

import (
	"errors"
	"math/bits"
)

const (
	alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	// Length is the length of every generated code.
	Length = 7
	// Space is the number of distinct 7-character codes: 62^7 (about 3.5 trillion).
	Space uint64 = 3_521_614_606_208

	// multiplier must be coprime with Space (= 2^7 * 31^7): odd and not a
	// multiple of 31. That makes x -> (x*multiplier + offset) mod Space a bijection.
	multiplier uint64 = 1_580_030_173
	offset     uint64 = 914_502_331_877
)

var (
	ErrOutOfSpace  = errors.New("shortid: id exceeds code space")
	ErrInvalidCode = errors.New("shortid: invalid code")
)

var decodeTable = func() [256]int8 {
	var t [256]int8
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(alphabet); i++ {
		t[alphabet[i]] = int8(i)
	}
	return t
}()

// Scramble maps id to a pseudo-random position in the code space. Distinct ids
// always map to distinct results.
func Scramble(id uint64) (uint64, error) {
	if id >= Space {
		return 0, ErrOutOfSpace
	}
	hi, lo := bits.Mul64(id, multiplier)
	r := bits.Rem64(hi, lo, Space)
	return (r + offset) % Space, nil
}

// Encode returns the fixed-width base62 representation of n (n < Space).
func Encode(n uint64) string {
	var buf [Length]byte
	for i := Length - 1; i >= 0; i-- {
		buf[i] = alphabet[n%62]
		n /= 62
	}
	return string(buf[:])
}

// Decode is the inverse of Encode.
func Decode(s string) (uint64, error) {
	if len(s) != Length {
		return 0, ErrInvalidCode
	}
	var n uint64
	for i := 0; i < len(s); i++ {
		d := decodeTable[s[i]]
		if d < 0 {
			return 0, ErrInvalidCode
		}
		n = n*62 + uint64(d)
	}
	return n, nil
}

// FromID scrambles and encodes id in one step.
func FromID(id uint64) (string, error) {
	s, err := Scramble(id)
	if err != nil {
		return "", err
	}
	return Encode(s), nil
}
