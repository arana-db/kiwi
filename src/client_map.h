#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include "client.h"

namespace kiwi {
class ClientMap {
 private:
  ClientMap() = default;
  // 禁用复制构造函数和赋值运算符

 private:
  std::map<int, std::weak_ptr<PClient>> clients_;
  std::shared_mutex client_map_mutex_;

 public:
  static ClientMap& getInstance() {
    static ClientMap instance;
    return instance;
  }

  ClientMap(const ClientMap&) = delete;
  ClientMap& operator=(const ClientMap&) = delete;

  // client info function
  kiwi::ClientInfo GetClientsInfoById(int id);
  uint32_t GetAllClientInfos(std::vector<ClientInfo>& results);

  bool AddClient(int id, std::weak_ptr<PClient>);

  bool RemoveClientById(int id);

  bool KillAllClients();
  bool KillClientById(int client_id);
  bool KillClientByAddrPort(const std::string& addr_port);
};

}  // namespace kiwi