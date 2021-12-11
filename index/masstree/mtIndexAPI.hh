#ifndef MTINDEXAPI_H
#define MTINDEXAPI_H

#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <limits.h>
#if HAVE_NUMA_H
#include <numa.h>
#endif
#if HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#if HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#if __linux__
#include <asm-generic/mman.h>
#endif
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#ifdef __linux__
#include <malloc.h>
#endif
#include "nodeversion.hh"
#include "kvstats.hh"
#include "query_masstree.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "timestamp.hh"
#include "json.hh"
#include "kvtest.hh"
#include "kvrandom.hh"
#include "kvrow.hh"
#include "kvthread.hh"
#include "kvio.hh"
#include "clp.h"
#include <algorithm>
#include <numeric>

#include <stdint.h>
#include "config.h"

#include "masstree_stats.hh"
#include "masstree_struct.hh"

#define GC_THRESHOLD 1000000
#define MERGE 0
#define MERGE_THRESHOLD 1000000
#define MERGE_RATIO 10
#define VALUE_LEN 8

#define LITTLEENDIAN 1
#define BITS_PER_KEY 8
#define K 2

#define SECONDARY_INDEX_TYPE 1

template <typename T>
class mt_index {
public:
  mt_index() {}
  ~mt_index() {
/*
    table_->destroy(*ti_);
    delete table_;
    ti_->rcu_clean();
    ti_->deallocate_ti();
    free(ti_);
    if (cur_key_)
      free(cur_key_);
*/    
    return;
  }

  //#####################################################################################
  // Initialize
  //#####################################################################################
  unsigned long long rdtsc_timer() {
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((unsigned long long)hi << 32) | lo;
  }

  inline void setup(threadinfo *ti) {
    table_ = new T;
    table_->initialize(*ti);

    srand(rdtsc_timer());
  }

  //#####################################################################################
  // Garbage Collection
  //#####################################################################################
  inline void clean_rcu(threadinfo *ti) {
    ti->rcu_quiesce();
  }

  // inline void gc_dynamic(threadinfo *ti) {
  //   if (ti->limbo >= GC_THRESHOLD) {
  //     clean_rcu(ti);
  //     ti->dealloc_rcu += ti->limbo;
  //     ti->limbo = 0;
  //   }
  // }
  
  //#####################################################################################
  //Insert Unique
  //#####################################################################################
  inline bool put_uv(const Str &key, const Str &value, threadinfo *ti) {
    #ifdef BREAKDOWN
    Masstree::abort = false;
    uint64_t start, end;
    #endif
    typename T::cursor_type lp(table_->table(), key);
    bool found = lp.find_insert(*ti);
    #ifdef BREAKDOWN
    start = Masstree::_rdtsc();
    #endif
    if (!found)
      ti->observe_phantoms(lp.node());
    else {
      #ifdef BREAKDOWN
      end = Masstree::_rdtsc();
      Masstree::time_node += (end - start);
      #endif
      lp.finish(1, *ti);
      return false;
    }
    qtimes_.ts = ti->update_timestamp();
    qtimes_.prev_ts = 0;
    lp.value() = row_type::create1(value, qtimes_.ts, *ti);
    #ifdef BREAKDOWN
    end = Masstree::_rdtsc();
    Masstree::time_node += (end - start);
    #endif
    lp.finish(1, *ti);

    return true;
  }
  
  bool put_uv(const char *key, int keylen, const char *value, int valuelen, threadinfo *ti) {
    return put_uv(Str(key, keylen), Str(value, valuelen), ti);
  }

  //#####################################################################################
  // Upsert
  //#####################################################################################
  inline void put(const Str &key, const Str &value, threadinfo *ti) {
    #ifdef BREAKDOWN
    Masstree::abort = false;
    uint64_t start, end;
    #endif
    typename T::cursor_type lp(table_->table(), key);
    bool found = lp.find_insert(*ti);
    #ifdef BREAKDOWN
    start = Masstree::_rdtsc();
    #endif
    if (!found) {
      ti->observe_phantoms(lp.node());
      qtimes_.ts = ti->update_timestamp();
      qtimes_.prev_ts = 0;
    }
    else {
      qtimes_.ts = ti->update_timestamp(lp.value()->timestamp());
      qtimes_.prev_ts = lp.value()->timestamp();
      lp.value()->deallocate_rcu(*ti);
    }
    
    lp.value() = row_type::create1(value, qtimes_.ts, *ti);
    #ifdef BREAKDOWN
    end = Masstree::_rdtsc();
    Masstree::time_node += (end - start);
    #endif
    lp.finish(1, *ti);
  }

  void put(const char *key, int keylen, const char *value, int valuelen, threadinfo *ti) {
    put(Str(key, keylen), Str(value, valuelen), ti);
  }

  //#################################################################################
  // Get (unique value)
  //#################################################################################
  inline bool dynamic_get(const Str &key, Str &value, threadinfo *ti) {
    #ifdef BREAKDOWN
    Masstree::abort = false;
    uint64_t start, end;
    #endif
    typename T::unlocked_cursor_type lp(table_->table(), key);
    bool found = lp.find_unlocked(*ti);
    #ifdef BREAKDOWN
    start = Masstree::_rdtsc();
    #endif
    if (found)
      value = lp.value()->col(0);
    #ifdef BREAKDOWN
    end = Masstree::_rdtsc();
    Masstree::time_node += (end - start);
    #endif
    return found;
  }
  
  bool get (const char *key, int keylen, Str &value, threadinfo *ti) {
    return dynamic_get(Str(key, keylen), value, ti);
  }

  //#################################################################################
  // Get Next (ordered)
  //#################################################################################
  bool dynamic_get_next(Str &value, char *cur_key, int *cur_keylen, threadinfo *ti) {
    Json req = Json::array(0, 0, Str(cur_key, *cur_keylen), 2);
    q_[0].run_scan(table_->table(), req, *ti);
    if (req.size() < 4)
      return false;
    value = req[3].as_s();
    if (req.size() < 6) {
      *cur_keylen = 0;
    }
    else {
      Str cur_key_str = req[4].as_s();
      memcpy(cur_key, cur_key_str.s, cur_key_str.len);
      *cur_keylen = cur_key_str.len;
    }
    
    return true;
  }

  //#################################################################################
  // Get Next N (ordered)
  //#################################################################################
  struct scanner {
    Str *values;
    int range;

    scanner(Str *values, int range)
      : values(values), range(range) {
    }

    template <typename SS2, typename K2>
    void visit_leaf(const SS2&, const K2&, threadinfo&) {}
    bool visit_value(Str key, const row_type* row, threadinfo&) {
        *values = row->col(0);
        ++values;
        --range;
        return range > 0;
    }
  };
  int get_next_n(Str *values, char *cur_key, int *cur_keylen, int range, threadinfo *ti) {
    if (range == 0)
      return 0;
    #ifdef BREAKDOWN
    Masstree::abort = false;
    #endif

    scanner s(values, range);
    int count = table_->table().scan(Str(cur_key, *cur_keylen), true, s, *ti);
    return count;
  }

  #ifdef BREAKDOWN
  void get_breakdown(uint64_t& _time_traversal, uint64_t& _time_abort, uint64_t& _time_latch, uint64_t& _time_node, uint64_t& _time_split){
      Masstree::get_breakdown(_time_traversal, _time_abort, _time_latch, _time_node, _time_split);
  }
  #endif

  void measure_footprint_without_suffix(Masstree::node_base<Masstree::default_query_table_params>* node, uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){ 
      using default_table_ = Masstree::default_query_table_params;
      if(!node){
	  return;
      }
      else if(node->isleaf()){
	  auto leafnode = static_cast<Masstree::leaf<default_table_>*>(node);
	  auto meta_ = sizeof(Masstree::node_base<default_table_>) + 			// base node
	      	       sizeof(int8_t) +							// extrasize64_
		       sizeof(uint8_t) + 						// modestate_
		       sizeof(uint8_t) * Masstree::leaf<default_table_>::width +	// keylenx_[width]
		       sizeof(kpermuter<default_table_::leaf_width>) +			// permutation_
		       sizeof(uintptr_t) + 						// next_
		       sizeof(Masstree::leaf<default_table_>*) + 			// prev_
		       sizeof(Masstree::node_base<default_table_>*) +			// parent_
		       sizeof(default_table_::phantom_epoch_type)*default_table_::need_phantom_epoch; // epoch

	  meta += meta_;

	  typename Masstree::leaf<default_table_>::permuter_type perm(leafnode->permutation_);
	  auto size = perm.size();
	  auto invalid_num = Masstree::leaf<default_table_>::width - perm.size();
	  if(invalid_num != 0){
	      structural_data_unoccupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type) * invalid_num;
	      key_data_unoccupied += sizeof(Masstree::leaf<default_table_>::ikey_type) * invalid_num;
	  }
	  key_data_occupied += sizeof(Masstree::leaf<default_table_>::ikey_type) * size; // keys

	  for(int i=0; i<size; i++){
	      if(leafnode->is_layer(perm[i])){
		  structural_data_occupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type); // ptr to lower layer
		  measure_footprint_without_suffix(leafnode->lv_[perm[i]].layer(), meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	      }
	      else{
		  key_data_occupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type); // value
	      }
	  }
      }
      else{
	  auto internalnode = static_cast<Masstree::internode<default_table_>*>(node);
	  auto meta_ = sizeof(Masstree::node_base<default_table_>) +		// base node
		       sizeof(uint8_t) + 					// nkeys_
		       sizeof(uint32_t) + 					// height_
		       sizeof(Masstree::node_base<default_table_>*);		// parent_
	  meta += meta_;

	  auto size = internalnode->size();
	  auto invalid_num = Masstree::internode<default_table_>::width - size;
	  if(invalid_num != 0)
	      structural_data_unoccupied += (sizeof(Masstree::internode<default_table_>::ikey_type) + sizeof(Masstree::node_base<default_table_>*)) * invalid_num;
	  structural_data_occupied += sizeof(Masstree::internode<default_table_>::ikey_type) * size + sizeof(Masstree::node_base<default_table_>*) * (size + 1);

	  for(int i=0; i<=size; i++){
	      if(internalnode->child_[i])
		  measure_footprint_without_suffix(internalnode->child_[i], meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	  }
      }
  }



  void measure_footprint_with_suffix(Masstree::node_base<Masstree::default_query_table_params>* node, uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){ 
      using default_table_ = Masstree::default_query_table_params;
      if(!node){
	  return;
      }
      else if(node->isleaf()){
	  auto leafnode = static_cast<Masstree::leaf<default_table_>*>(node);
	  auto meta_ = sizeof(Masstree::node_base<default_table_>) + 			// base node
	      	       sizeof(int8_t) +							// extrasize64_
		       sizeof(uint8_t) + 						// modestate_
		       sizeof(uint8_t) * Masstree::leaf<default_table_>::width +	// keylenx_[width]
		       sizeof(kpermuter<default_table_::leaf_width>) +			// permutation_
		       sizeof(uintptr_t) + 						// next_
		       sizeof(Masstree::leaf<default_table_>*) + 			// prev_
		       sizeof(Masstree::node_base<default_table_>*) +			// parent_
		       sizeof(default_table_::phantom_epoch_type)*default_table_::need_phantom_epoch; // epoch

	  meta += meta_;

	  typename Masstree::leaf<default_table_>::permuter_type perm(leafnode->permutation_);
	  auto size = perm.size();
	  auto invalid_num = Masstree::leaf<default_table_>::width - perm.size();
	  if(invalid_num != 0){
	      structural_data_unoccupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type) * invalid_num;
	      key_data_unoccupied += sizeof(Masstree::leaf<default_table_>::ikey_type) * invalid_num;
	  }
	  key_data_occupied += sizeof(Masstree::leaf<default_table_>::ikey_type) * size; // keys

	  bool has_suffix = false;
	  for(int i=0; i<size; i++){
	      if(leafnode->is_layer(perm[i])){
		  structural_data_occupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type); // ptr to lower layer
		  measure_footprint_with_suffix(leafnode->lv_[perm[i]].layer(), meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	      }
	      else{
		  if(leafnode->has_ksuf(perm[i])){
		      has_suffix = true;
		      auto suffix = leafnode->ksuf(perm[i]);
		      key_data_occupied += suffix.len;
		  }
		  key_data_occupied += sizeof(Masstree::leaf<default_table_>::leafvalue_type); // value
	      }
	  }
	  if(has_suffix){
	      if(leafnode->ksuf_)
		  meta += sizeof(Masstree::leaf<default_table_>::external_ksuf_type);
	      else
		  meta += sizeof(Masstree::leaf<default_table_>::internal_ksuf_type);
	  }
      }
      else{
	  auto internalnode = static_cast<Masstree::internode<default_table_>*>(node);
	  auto meta_ = sizeof(Masstree::node_base<default_table_>) +		// base node
		       sizeof(uint8_t) + 					// nkeys_
		       sizeof(uint32_t) + 					// height_
		       sizeof(Masstree::node_base<default_table_>*);		// parent_
	  meta += meta_;

	  auto size = internalnode->size();
	  auto invalid_num = Masstree::internode<default_table_>::width - size;
	  if(invalid_num != 0)
	      structural_data_unoccupied += (sizeof(Masstree::internode<default_table_>::ikey_type) + sizeof(Masstree::node_base<default_table_>*)) * invalid_num;
	  structural_data_occupied += sizeof(Masstree::internode<default_table_>::ikey_type) * size + sizeof(Masstree::node_base<default_table_>*) * (size + 1);

	  for(int i=0; i<=size; i++){
	      if(internalnode->child_[i])
		  measure_footprint_with_suffix(internalnode->child_[i], meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	  }
      }
  }


  void get_footprint_with_suffix(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
      measure_footprint_with_suffix(table_->table().root(), meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
  }

  void get_footprint_without_suffix(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
      measure_footprint_without_suffix(table_->table().root(), meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
  }

  void utilization(Masstree::node_base<Masstree::default_query_table_params>* node, uint64_t& internalnode_total, uint64_t& leafnode_total, uint64_t& internalnode_valid, uint64_t& leafnode_valid){
      using default_table_ = Masstree::default_query_table_params;
      if(!node)
	  return;
      else if(node->isleaf()){
	  auto leafnode = static_cast<Masstree::leaf<default_table_>*>(node);
	  typename Masstree::leaf<default_table_>::permuter_type perm(leafnode->permutation_);
	  leafnode_total += Masstree::leaf<default_table_>::width;
	  leafnode_valid += perm.size();
	  for(int i=0; i<perm.size(); i++){
	      if(leafnode->is_layer(perm[i]))
		  utilization(leafnode->lv_[perm[i]].layer(), internalnode_total, leafnode_total, internalnode_valid, leafnode_valid);
	  }
      }
      else{
	  auto internalnode = static_cast<Masstree::internode<default_table_>*>(node);
	  internalnode_total += Masstree::internode<default_table_>::width;
	  internalnode_valid += internalnode->size();
	  for(int i=0; i<=internalnode->size(); i++){
	      if(internalnode->child_[i])
		  utilization(internalnode->child_[i], internalnode_total, leafnode_total, internalnode_valid, leafnode_valid);
	  }
      }
  }

  void utilization(double& internalnode_util, double& leafnode_util){
      uint64_t internalnode_total, leafnode_total;
      uint64_t internalnode_valid, leafnode_valid;
      internalnode_total = leafnode_total = internalnode_valid = leafnode_valid = 0;

      utilization(table_->table().root(), internalnode_total, leafnode_total, internalnode_valid, leafnode_valid);

      internalnode_util = (double)internalnode_valid / internalnode_total * 100;
      leafnode_util = (double)leafnode_valid / leafnode_total * 100;
  }

  void find_depth(Masstree::node_base<Masstree::default_query_table_params>* node, uint64_t& max_depth, uint64_t cur_depth, uint64_t& layer_count, uint64_t cur_layer){
      using default_table_ = Masstree::default_query_table_params;
      if(!node)
	  return;
      else if(node->isleaf()){
	  if(layer_count < cur_layer)
	      layer_count = cur_layer;
	  auto leafnode = static_cast<Masstree::leaf<default_table_>*>(node);
	  typename Masstree::leaf<default_table_>::permuter_type perm(leafnode->permutation_);
	  for(int i=0; i<perm.size(); i++){
	      if(leafnode->is_layer(perm[i])){
		  find_depth(leafnode->lv_[perm[i]].layer(), max_depth, cur_depth+1, layer_count, cur_layer+1);
	      }
	      else{
		  if(max_depth < cur_depth)
		      max_depth = cur_depth;
	      }
	  }
      }
      else{
	  if(layer_count < cur_layer)
	      layer_count = cur_layer;
	  auto internalnode = static_cast<Masstree::internode<default_table_>*>(node);
	  for(int i=0; i<=internalnode->size(); i++){
	      if(internalnode->child_[i])
		  find_depth(internalnode->child_[i], max_depth, cur_depth+1, layer_count, cur_layer);
	  }
      }
  }
  
  void find_depth(uint64_t& layer_num, uint64_t& max_depth){
      find_depth(table_->table().root(), max_depth, 1, layer_num, 1);
  }


private:
  T *table_;
  query<row_type> q_[1];
  loginfo::query_times qtimes_;
};

#endif //MTINDEXAPI_H
