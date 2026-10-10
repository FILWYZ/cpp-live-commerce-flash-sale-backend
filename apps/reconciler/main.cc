#include "application.h"

#include <cstdlib>

int main(int argc, char** argv) {
    (void)::setenv("ECOMMERCE_PROCESS_ROLE", "reconciler", 1);
    return runEcommerceApplication(argc, argv);
}
