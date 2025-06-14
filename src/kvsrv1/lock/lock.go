package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey  string // the key to store lock state
	clientID string // unique identifier for this client
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockKey:  l,
		clientID: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// Try to get the current lock state
		value, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			// Lock doesn't exist, try to create it with our client ID
			if putErr := lk.ck.Put(lk.lockKey, lk.clientID, 0); putErr == rpc.OK {
				// Successfully acquired the lock
				return
			}
			// Someone else created the lock first, try again
			continue
		} else if err == rpc.OK {
			// Lock exists, check if we already own it
			if value == lk.clientID {
				// We already own the lock
				return
			}
			// Lock is held by someone else, check if it's been released
			if value == "" {
				// Lock is released, try to acquire it
				if putErr := lk.ck.Put(lk.lockKey, lk.clientID, version); putErr == rpc.OK {
					// Successfully acquired the lock
					return
				}
				// Someone else acquired it first, try again
				continue
			}
			// Lock is held by someone else, wait and try again
			continue
		}
		// Get failed, retry
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// Get the current lock state
		value, version, err := lk.ck.Get(lk.lockKey)

		if err == rpc.ErrNoKey {
			// Lock doesn't exist, nothing to release
			return
		} else if err == rpc.OK {
			// Check if we own the lock
			if value == lk.clientID {
				// We own the lock, release it by setting value to empty
				if putErr := lk.ck.Put(lk.lockKey, "", version); putErr == rpc.OK {
					// Successfully released the lock
					return
				}
				// Put failed (version mismatch), try again
				continue
			}
			// We don't own the lock, nothing to release
			return
		}
		// Get failed, retry
	}
}
