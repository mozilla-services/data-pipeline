/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @brief Lua Firefox broken sessions cuckoo filter implementation @file */

#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "../luasandbox_serialize.h"
#include "common.h"
#include "lauxlib.h"
#include "lua.h"
#include "xxhash.h"

static const char* mozsvc_fxbs = "mozsvc.fx.broken_sessions";
static const char* mozsvc_fxbs_table = "fx.broken_sessions";

typedef struct bs_data {
  unsigned char last_consecutive;
  unsigned char missing; // tracks the number of missing session submissions
                         // if it exceeds eight the clientId will be flagged
                         // for investigation.
} bs_data;

typedef struct bs_bucket
{
  unsigned short entries[BUCKET_SIZE];
  bs_data data[BUCKET_SIZE];
} bs_bucket;

typedef struct fxbs
{
  size_t items;
  size_t bytes;
  size_t num_buckets;
  size_t cnt;
  int nlz;
  bs_bucket buckets[];
} fxbs;

static int fxbs_new(lua_State* lua)
{
  int n = lua_gettop(lua);
  luaL_argcheck(lua, n == 1, 0, "incorrect number of arguments");
  int items = luaL_checkint(lua, 1);
  luaL_argcheck(lua, 4 < items, 1, "items must be > 4");

  unsigned buckets = clp2((unsigned)ceil(items / BUCKET_SIZE));
  size_t bytes = sizeof(bs_bucket) * buckets;
  size_t nbytes = sizeof(fxbs) + bytes;
  fxbs* cf = (fxbs*)lua_newuserdata(lua, nbytes);
  cf->items = buckets * BUCKET_SIZE;
  cf->num_buckets = buckets;
  cf->bytes = bytes;
  cf->cnt = 0;
  cf->nlz = nlz(buckets) + 1;
  memset(cf->buckets, 0, cf->bytes);

  luaL_getmetatable(lua, mozsvc_fxbs);
  lua_setmetatable(lua, -2);
  return 1;
}


static fxbs* check_fxbs(lua_State* lua, int args)
{
  void* ud = luaL_checkudata(lua, 1, mozsvc_fxbs);
  luaL_argcheck(lua, ud != NULL, 1, "invalid userdata type");
  luaL_argcheck(lua, args == lua_gettop(lua), 0,
                "incorrect number of arguments");
  return (fxbs*)ud;
}


static bool bucket_query_lookup(bs_bucket* b, unsigned fp)
{
  for (int i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == fp) {
      return true;
    }
  }
  return false;
}


static int bucket_insert_lookup(bs_bucket* b, unsigned fp, bs_data* data)
{
  for (int i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == fp) {
      if (data->last_consecutive - b->data[i].last_consecutive == 1) {
        b->data[i].last_consecutive = data->last_consecutive;
        if (b->data[i].missing) {
          b->data[i].missing >>= 1;
          while (b->data[i].missing & 1) {
            b->data[i].missing >>= 1;
            ++b->data[i].last_consecutive;
          }
        }
        return 1; // found and correct
      } else if (data->last_consecutive <= b->data[i].last_consecutive) {
        return 4; // duplicate
      } else if (data->last_consecutive - b->data[i].last_consecutive <= 8) {
        b->data[i].missing |= 1 << (data->last_consecutive
                                    - b->data[i].last_consecutive - 1);
        return 2; // out of order
      } else {
        // advance to the current counter
        b->data[i].last_consecutive = data->last_consecutive;
        b->data[i].missing = 0;
        return 5; // too many missing subsessions
      }
    }
  }
  return 0;
}


static bool bucket_delete(bs_bucket* b, unsigned fp)
{
  for (int i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == fp) {
      b->entries[i] = 0;
      memset(&b->data[i], 0, sizeof(bs_data));
      return true;
    }
  }
  return false;
}


static bool bucket_add(bs_bucket* b, unsigned fp, bs_data* data)
{
  for (int i = 0; i < BUCKET_SIZE; ++i) {
    if (b->entries[i] == 0) {
      b->entries[i] = fp;
      b->data[i] = *data;
      return true;
    }
  }
  return false;
}


static int bucket_insert(fxbs* cf, unsigned i1, unsigned i2, unsigned fp,
                         bs_data* data)
{
  // since we must handle duplicates we consider any collision within the bucket
  // to be a duplicate. The 16 bit fingerprint makes the false postive rate very
  // low 0.00012
  int res = bucket_insert_lookup(&cf->buckets[i1], fp, data);
  if (res) return res;
  res = bucket_insert_lookup(&cf->buckets[i2], fp, data);
  if (res) return res;

  if (!bucket_add(&cf->buckets[i1], fp, data)) {
    if (!bucket_add(&cf->buckets[i2], fp, data)) {
      unsigned ri;
      if (rand() % 2) {
        ri = i1;
      } else {
        ri = i2;
      }
      bs_data tmpdata;
      for (int i = 0; i < 512; ++i) {
        int entry = rand() % BUCKET_SIZE;
        unsigned tmp = cf->buckets[ri].entries[entry];
        tmpdata = cf->buckets[ri].data[entry];
        cf->buckets[ri].entries[entry] = fp;
        cf->buckets[ri].data[entry] = *data;
        fp = tmp;
        data = &tmpdata;
        ri = ri ^ (XXH32(&fp, sizeof(unsigned), 1) >> cf->nlz);
        res = bucket_insert_lookup(&cf->buckets[ri], fp, data);
        if (res) return res;
        if (bucket_add(&cf->buckets[ri], fp, data)) {
          return 0;
        }
      }
      return -1;
    }
  }
  return 0;
}


static int fxbs_add(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 3);
  size_t len = 0;
  double val = 0;
  unsigned country, channel, os, day, dflt;
  if (lua_type(lua, 2) != LUA_TSTRING) {
    return luaL_argerror(lua, 2, "must be a string");
  }
  const char* key = lua_tolstring(lua, 2, &len);
  unsigned session_cnt = 0;
  if (lua_type(lua, 3) == LUA_TNUMBER) {
    session_cnt = (unsigned)lua_tointeger(lua, 3);
    if (session_cnt > 255) {
      lua_pushinteger(lua, 3);
      return 1;
    }
  } else {
    return luaL_argerror(lua, 3, "must a number");
  }

  bs_data data;
  data.last_consecutive = session_cnt;
  data.missing = 0;

  unsigned h = XXH32(key, (int)len, 1);
  unsigned fp = fingerprint(h);
  unsigned i1 = h % cf->num_buckets;
  unsigned i2 = i1 ^ (XXH32(&fp, sizeof(unsigned), 1) >> cf->nlz);

  int res = bucket_insert(cf, i1, i2, fp, &data);
  if (res == 0) {
    ++cf->cnt;
  }
  // -1 = not added
  // 0 = added
  // 1 = update no issue
  // 2 = out of order
  // 3 = too many subsessions
  // 4 = duplicate
  // 5 = too many missing subsessions
  lua_pushinteger(lua, res);
  return 1;
}


static int fxbs_query(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 2);
  size_t len = 0;
  if (lua_type(lua, 2) != LUA_TSTRING) {
    return luaL_argerror(lua, 2, "must be a string");
  }
  const char* key = lua_tolstring(lua, 2, &len);
  unsigned h = XXH32(key, (int)len, 1);
  unsigned fp = fingerprint(h);
  unsigned i1 = h % cf->num_buckets;

  bs_data data;
  bool found = bucket_query_lookup(&cf->buckets[i1], fp);
  if (!found) {
    unsigned i2 = i1 ^ (XXH32(&fp, sizeof(unsigned), 1) >> cf->nlz);
    found = bucket_query_lookup(&cf->buckets[i2], fp);
  }
  lua_pushboolean(lua, found);
  return 1;
}


static int fxbs_delete(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 2);
  size_t len = 0;
  if (lua_type(lua, 2) != LUA_TSTRING) {
    return luaL_argerror(lua, 2, "must be a string");
  }
  const char* key = lua_tolstring(lua, 2, &len);
  unsigned h = XXH32(key, (int)len, 1);
  unsigned fp = fingerprint(h);
  unsigned i1 = h % cf->num_buckets;

  bool deleted = bucket_delete(&cf->buckets[i1], fp);
  if (!deleted) {
    unsigned i2 = i1 ^ (XXH32(&fp, sizeof(unsigned), 1) >> cf->nlz);
    deleted = bucket_delete(&cf->buckets[i2], fp);
  }
  if (deleted) {
    --cf->cnt;
  }
  lua_pushboolean(lua, deleted);
  return 1;
}


static int fxbs_count(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 1);
  lua_pushnumber(lua, cf->cnt);
  return 1;
}


static int fxbs_clear(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 1);
  memset(cf->buckets, 0, cf->bytes);
  cf->cnt = 0;
  return 0;
}


static int fxbs_fromstring(lua_State* lua)
{
  fxbs* cf = check_fxbs(lua, 3);
  cf->cnt = (size_t)luaL_checknumber(lua, 2);
  size_t len = 0;
  const char* values = luaL_checklstring(lua, 3, &len);
  if (len != cf->bytes) {
    return luaL_error(lua, "fromstring() bytes found: %d, expected %d", len,
                      cf->bytes);
  }
  memcpy(cf->buckets, values, len);
  return 0;
}


static void increment_column(lua_State* lua, int col)
{
  double val;
  lua_rawgeti(lua, -1, col);
  val = lua_tonumber(lua, -1);
  lua_pop(lua, 1);
  ++val;
  lua_pushnumber(lua, val);
  lua_rawseti(lua, -2, col);
}


static int serialize_fxbs(lua_State* lua)
{
  lsb_output_data* output = (lsb_output_data*)lua_touserdata(lua, -1);
  const char* key = (const char*)lua_touserdata(lua, -2);
  fxbs* cf = (fxbs*)lua_touserdata(lua, -3);
  if (!(output && key && cf)) {
    return 0;
  }
  if (lsb_appendf(output,
                  "if %s == nil then %s = %s.new(%u) end\n",
                  key,
                  key,
                  mozsvc_fxbs_table,
                  (unsigned)cf->items)) {
    return 1;
  }

  if (lsb_appendf(output, "%s:fromstring(%u, \"", key, (unsigned)cf->cnt)) {
    return 1;
  }
  if (lsb_serialize_binary(cf->buckets, cf->bytes, output)) return 1;
  if (lsb_appends(output, "\")\n", 3)) {
    return 1;
  }
  return 0;
}


static const struct luaL_reg fxbslib_f[] =
{
  { "new", fxbs_new },
  { NULL, NULL }
};


static const struct luaL_reg fxbslib_m[] =
{
  { "add", fxbs_add },
  { "query", fxbs_query },
  { "delete", fxbs_delete },
  { "count", fxbs_count },
  { "clear", fxbs_clear },
  { "fromstring", fxbs_fromstring }, // used for data restoration
  { NULL, NULL }
};


int luaopen_fx_broken_sessions(lua_State* lua)
{
  lua_newtable(lua);
  lsb_add_serialize_function(lua, serialize_fxbs);
  lua_replace(lua, LUA_ENVIRONINDEX);
  luaL_newmetatable(lua, mozsvc_fxbs);
  lua_pushvalue(lua, -1);
  lua_setfield(lua, -2, "__index");
  luaL_register(lua, NULL, fxbslib_m);
  luaL_register(lua, mozsvc_fxbs_table, fxbslib_f);
  return 1;
}
