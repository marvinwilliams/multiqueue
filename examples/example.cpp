#include "multififo/multififo.hpp"

#include <iostream>

int main() {
    multififo::MultiFifo<int> mf(4, 64, 1);
    auto handle = mf.get_handle();

    for (int i = 0; i < 100; ++i) {
        handle.try_push(i);
    }

    for (int i = 0; i < 100; ++i) {
        while (true) {
            auto value = handle.try_pop();
            if (value) {
                std::cout << *value << std::endl;
                break;
            }
        }
    }
    return 0;
}
