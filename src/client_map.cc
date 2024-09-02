#include "client_map.h"
#include "log.h"

namespace kiwi {

uint32_t ClientMap::GetAllClientInfos(std::vector<ClientInfo>& results) {
  // client info string type: ip, port, fd.
  std::shared_lock<std::shared_mutex> client_map_lock(client_map_mutex_);
  for (auto& [id, client_weak] : clients_) {
    if (auto client = client_weak.lock()) {
      results.emplace_back(client->GetClientInfo());
    }
  }
  return results.size();
}

bool ClientMap::AddClient(int id, std::weak_ptr<PClient> client) {
  std::unique_lock client_map_lock(client_map_mutex_);
  if (clients_.find(id) == clients_.end()) {
    clients_.insert({id, client});
    return true;
  }
  return false;
}

ClientInfo ClientMap::GetClientsInfoById(int id) {
  std::shared_lock client_map_lock(client_map_mutex_);
  if (auto it = clients_.find(id); it != clients_.end()) {
    if (auto client = it->second.lock(); client) {
      return client->GetClientInfo();
    }
  }
  ERROR("Client with ID {} not found in GetClientsInfoById", id);
  return ClientInfo::invalidClientInfo;
}

bool ClientMap::RemoveClientById(int id) {
  std::unique_lock client_map_lock(client_map_mutex_);
  if (auto it = clients_.find(id); it != clients_.end()) {
    clients_.erase(it);
    INFO("Removed client with ID {}", id);
    return true;
  }
  return false;
}

bool ClientMap::KillAllClients() {
  std::vector<std::shared_ptr<PClient>> clients_to_close;
  {
    std::shared_lock<std::shared_mutex> client_map_lock(client_map_mutex_);
    for (auto& [id, client_weak] : clients_) {
      if (auto client = client_weak.lock()) {
        clients_to_close.push_back(client);
      }
    }
  }
  for (auto& client : clients_to_close) {
    client->Close();
  }
  return true;
}

bool ClientMap::KillClientByAddrPort(const std::string& addr_port) {
  std::shared_ptr<PClient> client_to_close;
  {
    std::shared_lock<std::shared_mutex> client_map_lock(client_map_mutex_);
    for (auto& [id, client_weak] : clients_) {
      if (auto client = client_weak.lock()) {
        std::string client_ip_port = client->PeerIP() + ":" + std::to_string(client->PeerPort());
        if (client_ip_port == addr_port) {
          client_to_close = client;
          break;
        }
      }
    }
  }
  if (client_to_close) {
    client_to_close->Close();
    return true;
  }
  return false;
}

bool ClientMap::KillClientById(int client_id) {
  std::shared_ptr<PClient> client_to_close;
  {
    std::shared_lock<std::shared_mutex> client_map_lock(client_map_mutex_);
    if (auto it = clients_.find(client_id); it != clients_.end()) {
      if (auto client = it->second.lock()) {
        client_to_close = client;
      }
    }
  }
  if (client_to_close) {
    INFO("Closing client with ID {}", client_id);
    client_to_close->Close();
    INFO("Client with ID {} closed", client_id);
    return true;
  }
  return false;
}

}  // namespace kiwi