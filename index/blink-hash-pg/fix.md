## CRITICAL: Amortize Hash Scan Cost During Pending Conversion

### Problem
When ASYNC_ADAPT is enabled, a hash node that has been flagged CONVERT_PENDING 
but not yet converted will be scanned by `lnode_hash_t::range_lookup()` on every 
query. The collect+sort of ~15K entries takes ~400 µs, turning EVERY query into 
a spike rather than just the first one.

### Solution: Eager Inline Conversion for the First Scanner

Do NOT let the hash node serve range queries while unconverted. Instead, use a 
**hybrid approach**:

1. The FIRST scanner to hit an unconverted hash node still converts it 
   **synchronously** (same as today).
2. For nodes that are ALREADY being converted by a background worker 
   (`convert_state == CONVERT_ACTIVE`), fall through to the hash scan path.
3. Background workers handle nodes that were flagged by INSERT-heavy workloads 
   (where no range scan has hit them yet) or nodes where the first sync attempt 
   failed due to OLC conflict.

### Revised lnode.cpp Logic

```cpp
case HASH_NODE:
    #ifdef ADAPTATION
    #ifdef ASYNC_ADAPT
    if(sibling_ptr != nullptr){
        auto hash_node = static_cast<lnode_hash_t<Key_t, Value_t>*>(this);
        auto state = hash_node->convert_state.load(std::memory_order_acquire);
        
        if(state == lnode_hash_t<Key_t, Value_t>::CONVERT_ACTIVE){
            // Background worker is already converting — scan hash directly
            // (this is the rare case; pays the ~400µs cost but avoids blocking
            // on the convertlock that the worker holds)
            break;  // fall through to hash range_lookup below
        }
        
        // No one is converting yet — signal for background AND return -2
        // so the caller (tree::range_lookup) converts inline.
        // This gives us the best of both worlds:
        //   - First scan: converts inline (sync, ~500µs, one-time cost)
        //   - If inline convert fails (OLC): background worker retries
        uint8_t expected = lnode_hash_t<Key_t, Value_t>::CONVERT_NONE;
        hash_node->convert_state.compare_exchange_strong(
            expected,
            lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING,
            std::memory_order_acq_rel,
            std::memory_order_relaxed);
        return -2;  // caller does inline convert, same as sync path
    }
    #else
    if(sibling_ptr != nullptr)
        return -2;
    #endif
    #endif
    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->range_lookup(key, buf, count, range);
```

### Revised tree.cpp range_lookup Logic

```cpp
#ifdef ASYNC_ADAPT
    else if(ret == -2){
        // Try inline convert first (fast path — same as sync)
        auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
        if(!ret_){
            // OLC conflict — enqueue for background worker to retry
            signal_convert(static_cast<node_t*>(leaf), leaf_vstart);
        }
        goto restart;
    }
    // If we reach here with a hash node, a background worker is actively
    // converting it. The hash scan result is already in buf. Continue.
    if(leaf->type == lnode_t<Key_t, Value_t>::HASH_NODE){
        auto hash_leaf = static_cast<lnode_hash_t<Key_t, Value_t>*>(leaf);
        if(hash_leaf->convert_state.load(std::memory_order_acquire)
                == lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING){
            signal_convert(static_cast<node_t*>(leaf), leaf_vstart);
        }
    }
#else
    else if(ret == -2){
        auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
        goto restart;
    }
#endif
```