/*
 * config_parser.h
 *     Declared a set of functions for parsing configuration data
 * as required.
 *
 * Copyright (c) 2023-present, Arana/Kiwi Community All rights reserved.
 *
 * src/config_parser.h
 *
 */

#pragma once

#include <map>
#include <sstream>
#include <string>
#include <vector>

#ifdef CONFIG_DEBUG
#  include <iostream>
#endif

namespace kiwi {

class ConfigParser {
 public:
  using Data = std::map<std::string, std::vector<std::string>>;

  bool Load(const char* FileName);

  template <typename T>
  T GetData(const char* key, const T& default_ = T()) const;

  const std::vector<std::string>& GetDataVector(const char* key) const;

  const Data& GetMap() { return data_; }

#ifdef CONFIG_DEBUG
  void Print() {
    std::cout << "//////////////////" << std::endl;
    std::map<std::string, std::string>::const_iterator it = data_.begin();
    while (it != data_.end()) {
      std::cout << it->first << ":" << it->second << "\n";
      ++it;
    }
  }
#endif

 private:
  Data data_;

  template <typename T>
  T toType(const std::string& data) const;
};

template <typename T>
inline T ConfigParser::toType(const std::string& data) const {
  T t;
  std::istringstream os(data);
  os >> t;
  return t;
}

template <>
inline const char* ConfigParser::toType<const char*>(const std::string& data) const {
  return data.c_str();
}

template <>
inline std::string ConfigParser::toType<std::string>(const std::string& data) const {
  return data;
}

template <typename T>
inline T ConfigParser::GetData(const char* key, const T& default_) const {
  auto it = data_.find(key);
  if (it == data_.end()) {
    return default_;
  }

  return toType<T>(it->second[0]);  // only return first value
}

}  // namespace kiwi
