#include <jni.h>
#include <vector>
#include <iostream>

#include "include/nova_hetu_omnicache_OMVectorBase.h"

using namespace std;

JNIEXPORT jobject JNICALL Java_nova_hetu_omnicache_OMVectorBase_allocate
  (JNIEnv *env, jclass thisClass, jint size)
  {
    cout << "allocating with size: " << size <<endl;

    vector<char>* data = new vector<char>(size);
    jobject buf = env->NewDirectByteBuffer(data->data(), size);
    return env->NewGlobalRef(buf);
}

JNIEXPORT void JNICALL Java_nova_hetu_omnicache_OMVectorBase_mul
  (JNIEnv *env, jobject self, jint vectype, jobject[] data, jint m, jintArray param)
  {
  cout << "multiply by " << m <<endl;

  if(vectype == 1) { //Integer
    int* value = (int *)env->GetDirectBufferAddress(data);
    jlong capacity = env->GetDirectBufferCapacity(data);
    for (int i=0, e=capacity/sizeof(jint); i<e; i++) {
        cout << "old value: " << value[i] <<endl;
        value[i] = value[i] * m;
        cout << "new value: " << value[i] <<endl;
    }
  }

  if(vectype == 2) { //Long
      long* value = (long *)env->GetDirectBufferAddress(data);
      jlong capacity = env->GetDirectBufferCapacity(data);
      for (int i=0, e=capacity/sizeof(jlong); i<e; i++) {
          cout << "old value: " << value[i] <<endl;
          value[i] = value[i] * m;
          cout << "new value: " << value[i] <<endl;
      }
    }

}