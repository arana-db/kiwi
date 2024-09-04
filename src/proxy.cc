#include "proxy.h"

namespace pikiwidb {

PProxy& PProxy::Instance() {
  static PProxy inst_;
  return inst_;
}

bool ParseArgs(int ac, char* av[]) {
  // TODO: Parse ip from cfg file
}



}