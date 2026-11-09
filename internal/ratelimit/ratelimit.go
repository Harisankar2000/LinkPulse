// Package ratelimit is a distributed token-bucket rate limiter backed by Redis.
//
// The bucket state lives in Redis and is updated by a Lua script, so the
// read-modify-write is atomic and every API instance shares the same limits.
// The script uses Redis' own clock, so clock skew between instances can't
// hand out extra tokens.
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

var tokenBucket = redis.NewScript(`
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])   -- tokens added per second
local burst = tonumber(ARGV[2])   -- bucket capacity

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', key, math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry_ms}
`)

type Limiter struct {
	rdb   *redis.Client
	name  string
	rate  float64
	burst int
	log   *slog.Logger
}

// New returns a limiter allowing rate requests per second with bursts of up
// to burst. It returns nil when rate <= 0, which callers treat as "no limit".
func New(rdb *redis.Client, name string, rate float64, burst int, log *slog.Logger) *Limiter {
	if rate <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{rdb: rdb, name: name, rate: rate, burst: burst, log: log}
}

func (l *Limiter) Name() string { return l.name }

// Allow takes one token for key. When it is denied, retryAfter says when a
// token will be available. Redis errors fail open: a cache outage must not
// turn into an API outage.
func (l *Limiter) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration) {
	res, err := tokenBucket.Run(ctx, l.rdb, []string{"rl:" + l.name + ":" + key}, l.rate, l.burst).Int64Slice()
	if err != nil || len(res) != 2 {
		l.log.Warn("rate limiter unavailable, allowing request", "limiter", l.name, "err", err)
		return true, 0
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond
}
