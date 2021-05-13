#ifndef __DEBUG_H__
#define __DEBUG_H__

#include <chrono>

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::milliseconds ms;
typedef std::chrono::duration<float> fsec;

// define time 
#if defined(JNI_DEBUG) || defined(OP_DEBUG)
#define START() Time::now()
#define END(t0) \
    ({ \
        auto t1 = Time::now(); \
        fsec fs = t1 - t0; \
        ms d = std::chrono::duration_cast<ms>(fs); \
        d.count(); \
    })
#else
#define START() 0
#define END(t0) 0
#endif

// TODO define log level
#ifdef JNI_DEBUG
#define JNI_DEBUG_LOG(format, ...) printf("[%s][%s][%d]: " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define JNI_DEBUG_LOG(format, ...)
#endif

#ifdef OP_DEBUG
#define OP_DEBUG_LOG(format, ...) printf("[%s][%s][%d]: " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define OP_DEBUG_LOG(format, ...)
#endif

#define DebugPrintval(x) \
printf("[%s][%s][%d]:%s=%d\n", __FILE__, __FUNCTION__, __LINE__, #x, x)

#define DebugPrintStr(x) \
printf("[%s][%s][%d]:%s=%s\n", __FILE__, __FUNCTION__, __LINE__, #x, x)

#define DebugCr \
printf("\n")

#define DebugFuncEntry \
printf("[%s][%s][%d]:Func Entry\n", __FILE__, __FUNCTION__, __LINE__)

#define DebugFuncExit \
printf("[%s][%s][%d]:Func Exit\n", __FILE__, __FUNCTION__, __LINE__)

#define DebugPrint(format, ...)\
    do\
    {\
        printf("[%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__);\
        printf("\n");\
    }while(0);

#define DebugError(format, ...)\
    do\
    {\
        printf("[%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__);\
        printf("\n");\
    }while(0);

// #define OPTIMIZATION_BY_THREAD
#endif