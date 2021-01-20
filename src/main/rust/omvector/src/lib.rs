// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;
use bytebuffer::ByteBuffer;


// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString, JByteBuffer};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jint, jlong, jstring, jclass, jobject};

// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_RustNativeRunner_allocate(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                                      class: JClass,
                                                      input: JString)
                                                      -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String =
        env.get_string(input).expect("Couldn't get java string!").into();

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env.new_string(format!("Hello, {}!", input))
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_inner()
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    mul
 * Signature: (Ljava/nio/ByteBuffer;I)V
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_mul
(env: JNIEnv, this: jobject, value: jint) {

}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    mmul
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_mmul
(env: JNIEnv, this: jobject, that: jobject) {
}


/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    agg
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_agg
(env: JNIEnv, this: jobject, filter: jstring) -> jlong{
    let result = 0i64;
    result
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_allocate
(env: JNIEnv, _clazz: jclass, size: jint) -> JByteBuffer {
    let mut vec :[u8; 1024] = [0; 1024];
    for x in vec.iter() {
        print!("{}", x);
    }
    let buffer = env.new_direct_byte_buffer(vec.as_mut());
    let mut result = buffer.expect("Error allocating direct byte buffer");
    let mut newBuffer :ByteBuffer = ByteBuffer::from_bytes(env.get_direct_buffer_address(result).expect("error"));

    env.new_direct_byte_buffer(vec.as_mut()).expect("err")
}
