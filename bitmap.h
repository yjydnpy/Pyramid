#ifndef _BITMAP_H_
#define _BITMAP_H_

#include <stdlib.h>
#include <stdint.h>
#include <cstring>

#define BITS_PER_UNIT 8

#define BIT_MASK(pos) ((uint8_t)1 << (BITS_PER_UNIT - 1 - pos % BITS_PER_UNIT))
#define BIT_WORD(pos) ((pos) / BITS_PER_UNIT)

struct BitMap {
  BitMap(int c) {
    capacity = BIT_WORD(c);
    size = capacity;
    t = (uint8_t *)malloc(size);
    memset(t, 0, size);
  }
  BitMap(uint8_t *buf, int s) {
    size = s;
    capacity = size;
    t = buf;
  }
  int size;  // bits
  int capacity;   // bits
  uint8_t *t;
  void set_bit(int pos);
  void clear_bit(int pos);
  uint8_t test_bit(int pos);
  int find_first_zero_bit();
  void extend_size_with_set(int delta);
  void extend_size_with_clear(int delta);
  int find_first_nonzero_bihind(int pos);
  int find_first_zero_bihind(int pos);
};

void BitMap::set_bit(int pos) {
  uint8_t mask = BIT_MASK(pos);
  uint8_t *p = t + BIT_WORD(pos);
  *(p) |= mask;
}

void BitMap::clear_bit(int pos) {
  uint8_t mask = BIT_MASK(pos);
  uint8_t *p = t + BIT_WORD(pos);
  *(p) &= ~mask;
}

uint8_t BitMap::test_bit(int pos) {
  return BIT_MASK(pos) & *(uint8_t *)(t + BIT_WORD(pos));
}

int BitMap::find_first_zero_bit() {
  return 0;
}

static inline int drop(uint8_t u) {
  if (u < 0x80) {
    return 0;
  } else if (u < 0xC0) {
    return 1;
  } else if (u < 0xE0) {
    return 2;
  } else if (u < 0xF0) {
    return 3;
  } else if (u < 0xF8) {
    return 4;
  } else if (u < 0xFC) {
    return 5;
  } else if (u < 0xFE) {
    return 6;
  } else if (u < 0xFF) {
    return 7;
  } else {
    return -1;
  }
}

static inline int pick(uint8_t u) {
  if (u >= 0x80) {
    return 0;
  } else if (u >= 0x40) {
    return 1;
  } else if (u >= 0x20) {
    return 2;
  } else if (u >= 0x10) {
    return 3;
  } else if (u >= 0x08) {
    return 4;
  } else if (u >= 0x04) {
    return 5;
  } else if (u >= 0x02) {
    return 6;
  } else if (u >= 0x01) {
    return 7;
  } else {
    return -1;
  }
}

int BitMap::find_first_zero_bihind(int pos) {
  int i;
  int mod = (pos + 1) % BITS_PER_UNIT;
  if (mod) {
    uint8_t u = *(t + BIT_WORD(pos+1)) << mod;
    int p = drop(u); // 左移引入了0
    if (p != -1 && p < BITS_PER_UNIT - mod) {
      return pos + p + 1 < size ? pos + 1 + p : -1;
    }
  }
  int u_size = size / 8 + (size % 8 ? 1 : 0);
  int start = (pos + 1) / BITS_PER_UNIT + (mod ? 1 : 0);
  for (i = start; i < u_size; i++) {
    uint8_t u = *(t + i);
    if (u == 0xFF) continue;
    int p = drop(u);
    return p + i * BITS_PER_UNIT < size ? p + i * BITS_PER_UNIT : -1;
  }
  return -1;
}

int BitMap::find_first_nonzero_bihind(int pos) {
  int i;
  int mod = (pos + 1) % BITS_PER_UNIT;
  if (mod) {
    uint8_t u = *(t + BIT_WORD(pos+1)) << mod;
    int p = pick(u);
    if (p != -1) {
      return pos + p + 1 < size ? pos + 1 + p : -1;
    }
  }
  int u_size = size / 8 + (size % 8 ? 1 : 0);
  int start = (pos + 1) / BITS_PER_UNIT + (mod ? 1 : 0);
  for (i = start; i < u_size; i++) {
    uint8_t u = *(t + i);
    if (u == 0x00) continue;
    int p = pick(u);
    return p + i * BITS_PER_UNIT < size ? p + i * BITS_PER_UNIT : -1;
  }
  return -1;
}

void BitMap::extend_size_with_set(int delta) {
  int i;
  for (i = 0; i < delta; i++) {
    set_bit(size + i);
  }
  size += delta;
}

void BitMap::extend_size_with_clear(int delta) {
  int i;
  for (i = 0; i < delta; i++) {
    clear_bit(size + i);
  }
  size += delta;
}

#endif /* _BITMAP_H_ */
