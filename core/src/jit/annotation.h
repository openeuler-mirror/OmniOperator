#ifndef __OMNI_JIT_ANNOTATION_H__
#define __OMNI_JIT_ANNOTATION_H__

#define SUFFIX " annotation"
#define STRINGIFY(id) id SUFFIX
#define SPECIALIZE(id) __attribute__((annotate(STRINGIFY(id))))

#endif