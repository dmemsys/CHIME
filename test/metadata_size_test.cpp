#include "Common.h"


int main() {
  printf("avg. metadata size per item: %.4lf\n", (define::transLeafSize - (define::keyLen + define::inlineValLen) * define::leafSpanSize) * 1.0 / define::leafSpanSize);
  return 0;
}
