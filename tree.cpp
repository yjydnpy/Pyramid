/*
 * This is project Pyramid!
 * organize object data on file with Btree
 */

#include <string>
#include <map>
#include <set>
#include <cstdio>
#include <cstring>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include "bitmap.h"

using namespace std;

#define OK 0
#define INDEX_FULL 1
#define tree_degree 2
#define max_key_num tree_degree
#define max_child_num (tree_degree+1)
#define max_item_num 3

#define root_block_pid 1
#define root_node_pid 1
#define block_size 128
#define block_num_per_group 4
#define super_block_pid 0
#define item_str_size 16
#define REACH_FILE_TAIL -1

const char INIT_FLAG = 0xEF;

struct Item;
struct SuperNode;
struct IndexNode;
struct DataNode;
struct Engine;

/*
 * 1111000000001111000000000000
 *   ^ip  ^dp      ^size       ^cp
 */

/* TODO:
 * use template
 * use polymorphism
 */

static inline int read_block(int fd, int pid, char **str) {
  char *buf = (char *)malloc(block_size);
  lseek(fd, pid * block_size, SEEK_SET);
  int n = read(fd, buf, block_size);
  if (n == 0) {
    free(buf);
    return REACH_FILE_TAIL;
  }
  *str = buf;
  return 0;
}

static inline int write_block(int fd, int pid, char *str) {
  lseek(fd, pid * block_size, SEEK_SET);
  write(fd, str, block_size);
  return 0;
}

struct Item {
  Item() {}
  Item(int id, const char *str) : id(id) {
    strncpy(name, str, 12);
  }
  Item(const char *str) {
    memcpy(&id, str, 4);
    strncpy(name, str+4, 12);
  }
  Item(const Item &that) {
    id = that.id;
    strncpy(name, that.name, 12);
  }
  char* to_string(char *c) {
    memcpy(c, &id, 4);
    memcpy(c+4, name, 12);
    return c;
  }
  int id;
  char name[12];
}; /* struct Item */

struct DataNode {
  DataNode(int pid, Engine *e, char *str=NULL);
  Engine *e;
  int pid;
  int prev_node_pid = -1;
  int next_node_pid = -1;
  int keys[max_item_num];
  Item items[max_item_num];
  int key_num = 0;
  int min_max();
  void insert(int id, const Item &item);
  void print_self();
  void serialize_to_string(char *buf);
}; /* struct DataNode */

struct IndexNode {
  IndexNode(int pid, Engine *e, char *str=NULL);
  Engine *e;
  int pid;
  int keys[max_key_num];
  int childs[max_child_num];
  int key_num = 0;
  int child_num = 0;
  int high = 1; // 索引节点的高度
  int insert(int id, const Item &item);
  int get_min_max();
  DataNode* get_leftmost_data_node();
  DataNode* get_rightmost_data_node();
  void print_self();
  void serialize_to_string(char *buf);
}; /* struct IndexNode */

struct SuperNode {
  int size;
  int capacity;
  int index_position;
  int data_position;
  int fd;
  char *buf;
  BitMap *super_bitmap;

  SuperNode(int file_no);
  void flush_self();
  void flush_index_node(IndexNode *p);
  void flush_data_node(DataNode *p);
  int alloc_index_pid();
  int alloc_data_pid();
}; /* struct SupeerNode */

struct Engine {
  Engine() {}
  SuperNode *sn = NULL;
  int fd;
  map<int, DataNode*> data_nodes;
  map<int, IndexNode*> index_nodes;
  set<IndexNode*> dirty_index_nodes;
  set<DataNode*> dirty_data_nodes;
  IndexNode *root_node;

  void load_from_file(const char *file_name);
  DataNode* alloc_data_node();
  IndexNode* alloc_index_node();
  void insert(int id, const Item &item);
  DataNode* get_data_node(int pid);
  IndexNode* get_index_node(int pid);
  void exchange_pid(IndexNode *l, IndexNode *r);
  void print_self();
  void print_data_linked_list();
  bool is_data_node(int pid) { return !sn->super_bitmap->test_bit(pid); }
  bool is_index_node(int pid) { return sn->super_bitmap->test_bit(pid); }
  void mark_dirty_index_node(IndexNode *node);
  void mark_dirty_data_node(DataNode *node);
  void flush_to_file();
  void stop() {
    flush_to_file();
    close(fd);
  }
}; /* struct Engine */

void Engine::mark_dirty_data_node(DataNode *node) {
  dirty_data_nodes.insert(node);
}
void Engine::mark_dirty_index_node(IndexNode *node) {
  dirty_index_nodes.insert(node);
}

void Engine::flush_to_file() {
  sn->flush_self();
  auto it1 = dirty_index_nodes.begin();
  auto end1 = dirty_index_nodes.end();
  for (; it1 != end1; it1++) {
    sn->flush_index_node(*it1);
  }
  auto it2 = dirty_data_nodes.begin();
  auto end2 = dirty_data_nodes.end();
  for (; it2 != end2; it2++) {
    sn->flush_data_node(*it2);
  }
}
void Engine::load_from_file(const char *file_name) {
  int i, ret;
  fd = open(file_name, O_RDWR | O_CREAT, 0666);
  sn = new SuperNode(fd);
  root_node = get_index_node(root_node_pid);
  
}
void DataNode::serialize_to_string(char *buf) {
  int i;
  int offset = 0;
  memcpy(buf, &INIT_FLAG, 1);
  offset += 1;
  memcpy(buf + offset, &prev_node_pid, 4);
  offset += 4;
  memcpy(buf + offset, &next_node_pid, 4);
  offset += 4;
  memcpy(buf + offset, &key_num, 4);
  offset += 4;
  char c[item_str_size];
  for (i = 0; i < key_num; i++) {
    memset(c, 0, item_str_size);
    memcpy(buf+offset, items[i].to_string(c), item_str_size);
    offset += item_str_size;
  }
}

void DataNode::insert(int id, const Item &item) {
  printf("DataNode%d insert id %d, key_num %d\n", pid, id, key_num);
  keys[key_num] = id;
  items[key_num] = item;
  key_num++;
  e->mark_dirty_data_node(this);
}
int DataNode::min_max() {
  assert(key_num != 0);
  return keys[0] + max_item_num - 1;
}

void Engine::exchange_pid(IndexNode *l, IndexNode *r) {
  int lpid = l->pid;
  int rpid = r->pid;
  l->pid = rpid;
  r->pid = lpid;
  index_nodes[l->pid] = l;
  index_nodes[r->pid] = r;
}

void Engine::print_self() {
  root_node->print_self();
}

void Engine::print_data_linked_list() {
  printf("data_list: ");
  DataNode *p = root_node->get_leftmost_data_node();
  while (p) {
    printf("%d -> ", p->pid);
    int next_pid = p->next_node_pid;
    if (next_pid == -1) {
      printf("%d", -1);
      break;
    }
    p = get_data_node(next_pid);
  }
  printf("\n");
}

IndexNode::IndexNode(int pid, Engine *e, char *str) : pid(pid), e(e) {
  if (str && *(str) == INIT_FLAG) {
    int i;
    int *c = (int *)(str + 1);
    high = *(c++);
    key_num = *(c++);
    child_num = *(c++);
    for (i = 0; i < key_num; i++) {
      keys[i] = *(c++);
    }
    for (i = 0; i < child_num; i++) {
      childs[i] = *(c++);
    }
  }
  if (str) free(str);
}

void IndexNode::print_self() {
  int i;
  printf("Index pid is %d\n", pid);
  for (i = 0; i < key_num; i++) {
    printf("Index%d key %d st is %d\n", pid, i, keys[i]);
  }
  for (i = 0; i < child_num; i++) {
    printf("Index%d child %d st is %d\n", pid, i, childs[i]);
  }
  for (i = 0; i < child_num; i++) {
    int cid = childs[i];
    if (e->is_index_node(cid)) {
      IndexNode *p = e->get_index_node(cid);
      p->print_self();
    } else {
      DataNode *p = e->get_data_node(cid);
      p->print_self();
    }
  }
  return;
}

DataNode::DataNode(int pid, Engine *e, char *str) : pid(pid), e(e) {
  if (str && *(str) == INIT_FLAG) {
    int i;
    int *c = (int *)(str + 1);
    prev_node_pid = *(c++);
    next_node_pid = *(c++);
    key_num = *(c++);
    char *s = (char *)c;
    for (i = 0; i < key_num; i++) {
      items[i] = Item(s);
      keys[i] = items[i].id;
      s += item_str_size;
    }
  }
  if (str) {
    free(str);
  }
}

void DataNode::print_self() {
  int i;
  printf("Data pid is %d\n", pid);
  for (i = 0; i < key_num; i++) {
    printf("Data%d key %d st is %d\n", pid, i, keys[i]);
    printf("Data%d item %d st is Item{%d, %s}\n", pid, i, items[i].id, items[i].name);
  }
}

IndexNode* Engine::get_index_node(int pid) {
  if (index_nodes.find(pid) == index_nodes.end()) {
    char *str = NULL;
    read_block(fd, pid, &str);
    IndexNode *node = new IndexNode(pid, this, str);
    index_nodes[pid] = node;
    return node;
  } else {
    return index_nodes[pid];
  }
}

IndexNode* Engine::alloc_index_node() {
  int pid = sn->alloc_index_pid();
  IndexNode *node = new IndexNode(pid, this);
  index_nodes[pid] = node;
  mark_dirty_index_node(node);
  return node;
}

DataNode* Engine::get_data_node(int pid) {
  if (data_nodes.find(pid) == data_nodes.end()) {
    char *str = NULL;
    read_block(fd, pid, &str);
    DataNode *node = new DataNode(pid, this, str);
    data_nodes[pid] = node;
    return node;
  } else {
    return data_nodes[pid];
  }
}

DataNode* Engine::alloc_data_node() {
  int pid = sn->alloc_data_pid();
  DataNode *node = new DataNode(pid, this);
  data_nodes[pid] = node;
  mark_dirty_data_node(node);
  return node;
}

void Engine::insert(int id, const Item &item) {
  root_node->insert(id, item);
}

DataNode* IndexNode::get_leftmost_data_node() {
  if (high == 1) {
    assert(child_num >= 1);
    return e->get_data_node(childs[0]);
  }
  IndexNode *p = e->get_index_node(childs[0]);
  return p->get_leftmost_data_node();
}

DataNode* IndexNode::get_rightmost_data_node() {
  if (high == 1) {
    assert(child_num >= 1);
    return e->get_data_node(childs[child_num - 1]);
  }
  IndexNode *p = e->get_index_node(childs[child_num - 1]);
  return p->get_rightmost_data_node();
}

int IndexNode::insert(int id, const Item &item) {
  int i, ret;
  for (i = 0; i < key_num; i++) {
    if (keys[i] > id) break;
  }
  int pk = i;
  bool alloc_data = false, alloc_index = false;
  if (pk > child_num -1) {
    if (high > 1) {
      IndexNode *p = e->alloc_index_node();
      p->high = high - 1;
      childs[pk] = p->pid;
      child_num++;
      alloc_index = true;
    } else {
      DataNode *p = e->alloc_data_node();
      childs[pk] = p->pid;
      child_num++;
      alloc_data = true;
    }
  }
  int cid = childs[pk];
  if (high == 1) {
    DataNode *p = e->get_data_node(cid);
    p->insert(id, item);
    //OK;
  } else {
    IndexNode *p = e->get_index_node(cid);
    ret = p->insert(id, item);
    if (ret == INDEX_FULL) {
      if (key_num != max_key_num) {
        int min_max = p->get_min_max();
        keys[key_num] = min_max + 1;
        key_num++;
        e->mark_dirty_index_node(this);
      } else {
        if (pid != root_node_pid) {
          return INDEX_FULL;
        } else {
          IndexNode *p = e->alloc_index_node();
          p->high = high + 1;
          p->keys[0] = get_min_max() + 1;
          p->childs[0] = p->pid;
          p->key_num = 1;
          p->child_num = 1;
          e->exchange_pid(this, p);
          e->root_node = p;
          e->mark_dirty_index_node(this);
        }
      }
    }
  }
  if (alloc_data && pk > 0) {
    DataNode *prev_node = e->get_data_node(childs[pk - 1]);
    DataNode *next_node = e->get_data_node(childs[pk]);
    prev_node->next_node_pid = next_node->pid;
    next_node->prev_node_pid = prev_node->pid;
  }

  if (alloc_index && pk > 0) {
    IndexNode *prev_index = e->get_index_node(childs[pk - 1]);
    IndexNode *next_index = e->get_index_node(childs[pk]);
    DataNode *prev_node = prev_index->get_rightmost_data_node();
    DataNode *next_node = next_index->get_leftmost_data_node();
    prev_node->next_node_pid = next_node->pid;
    next_node->prev_node_pid = prev_node->pid;
  }

  if (alloc_data && key_num < child_num) {
    if (key_num == max_key_num) {
      if (pid != root_node_pid) {
        return INDEX_FULL;
      } else {
        IndexNode *p = e->alloc_index_node();
        p->high = high + 1;
        p->keys[0] = get_min_max() + 1;
        p->childs[0] = p->pid;
        p->key_num = 1;
        p->child_num = 1;
        e->exchange_pid(this, p);
        e->root_node = p;
        e->mark_dirty_index_node(this);
      }
    } else {
      int min_max = get_min_max();
      key_num++;
      keys[key_num-1] = min_max + 1;
      e->mark_dirty_index_node(this);
    }
  }

  return OK;
}

void IndexNode::serialize_to_string(char *buf) {
  int i;
  int offset = 0;
  memcpy(buf, &INIT_FLAG, 1);
  offset += 1;
  memcpy(buf + offset, &high, 4);
  offset += 4;
  memcpy(buf + offset, &key_num, 4);
  offset += 4;
  memcpy(buf + offset, &child_num, 4);
  offset += 4;
  for (i = 0; i < key_num; i++) {
    memcpy(buf + offset, &keys[i], 4);
    offset += 4;
  }
  for (i = 0; i < child_num; i++) {
    memcpy(buf + offset, &childs[i], 4);
    offset += 4;
  }
}

int IndexNode::get_min_max() {
  assert(child_num != 0);
  int cid = childs[child_num-1];
  if (e->is_data_node(cid)) {
    DataNode *p = e->get_data_node(cid);
    return p->min_max();
  } else {
    IndexNode *p = e->get_index_node(cid);
    return p->get_min_max();
  }
}

SuperNode::SuperNode(int file_no) {
  fd = file_no;
  buf = (char *)malloc(block_size);
  memset(buf, 0, block_size);
  char *str = NULL;
  read_block(fd, super_block_pid, &str);
  if (str && *str == INIT_FLAG) {
    int *c = (int *)(str + 1);
    index_position = *(c++);
    data_position = *(c++);
    size = *(c++);  // 决定文件大小 且要确保大小
    capacity = *(c++);
    super_bitmap = new BitMap((uint8_t *)c, size);
  } else {
    ftruncate(fd, 0);
    size = 1 + (block_num_per_group * 2);
    int init_alloc_size = block_size * size;
    fallocate(fd, 0, 0, init_alloc_size);
    index_position = root_block_pid + 1;
    data_position = block_num_per_group + 1;
    capacity = block_size - 17;
    uint8_t *c = (uint8_t *)malloc(capacity);
    super_bitmap = new BitMap(c, 1); // super_block is 0 
    super_bitmap->extend_size_with_set(block_num_per_group);
    super_bitmap->extend_size_with_clear(block_num_per_group);
  }
}

int SuperNode::alloc_index_pid() {
  int ret = index_position;
  int next_position = super_bitmap->find_first_nonzero_bihind(index_position);
  if (next_position == -1) {
    fallocate(fd, 0, size * block_size, block_num_per_group * block_size);
    size += block_num_per_group;
    super_bitmap->extend_size_with_set(block_num_per_group);
    next_position = super_bitmap->find_first_nonzero_bihind(index_position);
  }
  index_position = next_position;
  return ret;
}

int SuperNode::alloc_data_pid() {
  int ret = data_position;
  int next_position = super_bitmap->find_first_zero_bihind(data_position);
  if (next_position == -1) {
    fallocate(fd, 0, size * block_size, block_num_per_group * block_size);
    size += block_num_per_group;
    super_bitmap->extend_size_with_clear(block_num_per_group);
    next_position = super_bitmap->find_first_zero_bihind(data_position);
  }
  data_position = next_position;
  return ret;
}

void SuperNode::flush_self() {
  int offset = 0;
  memcpy(buf, &INIT_FLAG, 1);
  offset += 1;
  memcpy(buf+offset, &index_position, 4);
  offset += 4;
  memcpy(buf+offset, &data_position, 4);
  offset += 4;
  memcpy(buf+offset, &size, 4);
  offset += 4;
  memcpy(buf+offset, &capacity, 4);
  offset += 4;
  memcpy(buf+offset, super_bitmap->t, capacity);
  write_block(fd, super_block_pid, buf);
  memset(buf, 0, block_size);
}

void SuperNode::flush_index_node(IndexNode *p) {
  p->serialize_to_string(buf);
  write_block(fd, p->pid, buf);
  memset(buf, 0, block_size);
}

void SuperNode::flush_data_node(DataNode *p) {
  p->serialize_to_string(buf);
  write_block(fd, p->pid, buf);
  memset(buf, 0, block_size);
}

int main() {
  int i;
  Engine *db = new Engine();
  db->load_from_file("./data.db");

  int id = 50;
  const char *a[] = {"amy", "hexue", "yq", "bao", "zhao", "qiu", "xia", "yu", "ruo", "nan", "guo", "chen",
                     "yu", "zhang", "wan", "yi", "luo", "xun", "dan", "wang"};
  for (i = 0; i < sizeof(a)/sizeof(char *); i++) {
    Item item(id++, a[i]);
    //db->insert(item.id, item);
  }

  id = 80;
  const char *b[] = {"red", "white", "hair", "black", "computer", "beauty", "paper"};
  for (i = 0; i < sizeof(b)/sizeof(char *); i++) {
    Item item(id++, b[i]);
    //db->insert(item.id, item);
  }
  //db->print_self();
  db->print_data_linked_list();
  db->stop();
  return 0;
}

