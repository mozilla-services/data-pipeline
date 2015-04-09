/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @brief Lua hash functions @file */

#include "lauxlib.h"
#include "lua.h"
#include <zlib.h>

static int zlib_adler32(lua_State* lua)
{
  size_t len;
  const char* buf;

  if (lua_type(lua, 1) == LUA_TSTRING) {
    buf = lua_tolstring(lua, 1, &len);
  } else {
    return luaL_argerror(lua, 1, "must be a string");
  }

  uLong adler = adler32(0L, Z_NULL, 0);
  adler = adler32(adler, buf, len);
  lua_pushinteger(lua, adler);

  return 1;
}


static int zlib_crc32(lua_State* lua)
{
  size_t len;
  const char* buf;

  if (lua_type(lua, 1) == LUA_TSTRING) {
    buf = lua_tolstring(lua, 1, &len);
  } else {
    return luaL_argerror(lua, 1, "must be a string");
  }

  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buf, len);
  lua_pushinteger(lua, crc);

  return 1;
}


static const struct luaL_reg hashlib_f[] =
{
  { "adler32", zlib_adler32 }
  , { "crc32", zlib_crc32 }
  , { NULL, NULL }
};


int luaopen_hash(lua_State* lua)
{
  luaL_register(lua, "hash", hashlib_f);
  return 1;
}
