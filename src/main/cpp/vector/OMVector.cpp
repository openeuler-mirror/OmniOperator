#include <jni.h>
#include <iostream>
#include <vector>

#include "include/nova_hetu_omnicache_OMVectorBase.h"

using namespace std;

//SAMPLE IMPLEMENTATION OF THE JNI METHODS

JNIEXPORT jlongArray JNICALL Java_nova_hetu_omnicache_OMVector_get
  (JNIEnv *env, jobject thisObject)
  {
    cout << "hello" << endl;
    jlong result[] = {1, 1, 1};
    jlongArray ret = env->NewLongArray(3);
    env->SetLongArrayRegion(ret, 0, 3, result);
    return ret;
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omnicache_OMVector_allocate
  (JNIEnv *env, jobject thisObject, jint size)
  {
    vector<jfloat>* data = new vector<jfloat>(size, 10.0);
    jobject buf = env->NewDirectByteBuffer(data->data(), data->size() * sizeof(jfloat));
    return env->NewGlobalRef(buf);
}

JNIEXPORT void JNICALL Java_nova_hetu_omnicache_OMVector_mul
  (JNIEnv *env, jobject ThisObject, jobject data, jint m)
  {
  cout << "multiply by " << m <<endl;
    float* f = (float *)env->GetDirectBufferAddress(data);
    jlong capacity = env->GetDirectBufferCapacity(data);
    for (int i=0, e=capacity/sizeof(jfloat); i<e; i++) {
    cout << "old value: " << f[i] <<endl;
        f[i] = f[i] * m * i;
        cout << "new value: " << f[i] <<endl;
    }
}