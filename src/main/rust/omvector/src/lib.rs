use core::mem;

use jni::JNIEnv;
use jni::objects::{JClass, JByteBuffer, JValue, GlobalRef};
use jni::sys::{jclass, jint, jlong, jobject, jobjectArray, jstring, jvalue};
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::borrow::{Borrow, BorrowMut};

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    mul
 * Signature: (ILjava/nio/ByteBuffer;I)V
 */
#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_mul
(env: JNIEnv, calling_object: jobject, d_type: jint, data: JByteBuffer, multiplier: jint) {
    println!("multiple {} {}", d_type, multiplier);

    let buffer_ptr = env.get_direct_buffer_address(data);
    let buffer = buffer_ptr.expect("error getting buffer pointer");
    unsafe {
        if d_type == 1 { //int
            let mut vec = into::<i32>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier;
            }
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        } else if d_type == 2 { //long
            let mut vec = into::<i64>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier as i64;
            }
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        } else if d_type == 3 { //double
            let mut vec = into::<f64>(buffer);
            for i in 0..vec.len() {
                vec[i] *= multiplier as f64;
            }
            // println!("double multiply result: {:?}", vec);
            //ensure the memeory referenced by vec is not released
            mem::forget(vec);
        }
    }
}

/*
 * Class:     nova_hetu_omnicache_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_allocate
(env: JNIEnv, _clazz: JClass, size: jint) -> jobject {
    unsafe {
        let mut vec8 = vec![0u8; size as usize];
        let buffer = env.new_direct_byte_buffer(vec8.as_mut());
        let result = buffer.expect("Error allocating direct byte buffer").into_inner();
        mem::forget(vec8);
        result
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_nova_hetu_omnicache_OMVectorBase_free
(env: JNIEnv, this_class: JClass, buffer: JByteBuffer) {
    let buf_addr_result = env.get_direct_buffer_address(buffer);
    let buf_addr = buf_addr_result.expect("");
    unsafe {
        //taking the ownership of the buffer which will be released once out of scope
        Box::from_raw(buf_addr);
    }
}

/// Allocates an vector holds 1024 items of T
unsafe fn allocate_vec<T: Clone + Debug>(init: T) -> Vec<u8> {
    let mut vec = vec![init; 1024];
    let length = vec.len() * mem::size_of::<T>();
    let capacity = vec.capacity() * mem::size_of::<T>();
    let vec_ptr = vec.as_mut_ptr() as *mut u8;
    mem::forget(vec);
    Vec::from_raw_parts(vec_ptr, length, capacity)
}

/// Converts the [u8] array into a Vec[T]
/// This can be used to convert any JByteBuffer into target data types
unsafe fn into<T: Clone>(original: &mut [u8]) -> Vec<T> {
    if original.len() % mem::size_of::<T>() != 0 {
        panic!("Misaligned vector size, cannot convert vector of size {} to vector with element size {}", original.len(), mem::size_of::<T>());
    }

    let length = original.len() / mem::size_of::<T>();
    let capacity = length / mem::size_of::<T>();
    let result = original.as_mut_ptr() as *mut T;
    mem::forget(original); // don't run the destructor for vec32
    Vec::from_raw_parts(result, length, capacity)
}

unsafe fn into_u8<U: Clone>(original: &mut Vec<U>) -> Vec<u8> {
    println!("converting back to u8");
    let length = original.len() * mem::size_of::<U>();
    let capacity = original.capacity() * mem::size_of::<U>();
    let result = original.as_mut_ptr() as * mut u8;
    mem::forget(original);
    Vec::from_raw_parts(result, length, capacity)
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_allocate() {
        unsafe {
            let vec_i8 = allocate_vec::<i8>(0);
            assert_eq!(1024, vec_i8.len());
            let vec_i16 = allocate_vec::<i16>(0);
            assert_eq!(2048, vec_i16.len());
            let vec_i32 = allocate_vec::<i32>(0);
            assert_eq!(4096, vec_i32.len());
            let vec_i64 = allocate_vec::<i64>(0);
            assert_eq!(8192, vec_i64.len());
            let vec_f32 = allocate_vec::<f32>(0.);
            assert_eq!(4096, vec_f32.len());
            let vec_f64 = allocate_vec::<f64>(0.);
            assert_eq!(8192, vec_f64.len());
        }
    }

    #[test]
    fn test_multiple_allocate() {
        unsafe {
            verify_2_vec_alloc();
        }
    }

    #[test]
    fn test_multi_thread_allocate() {
        unsafe {
            let mut t_vec = vec![];
            for i in 0..24 {
                t_vec.push(thread::spawn(
                    move || {
                        for i in 0..1000000 {
                            verify_2_vec_alloc();
                        }
                        println!("finished: {}", i);
                    }));
            }

            for x in t_vec {
                x.join();
            }
        }
    }


unsafe fn verify_2_vec_alloc() {
    let mut vec_1 = allocate_vec::<i8>(0);
    let mut vec_2 = allocate_vec::<i8>(0);

    for i in 0..vec_1.len() {
        vec_1[i] = i as u8;
    }
    for i in 0..vec_1.len() {
        vec_2[i] = (i * 2) as u8;
    }

    for i in 0..vec_1.len() {
        assert_eq!(i as u8, vec_1[i] as u8);
    }
    for i in 0..vec_1.len() {
        assert_eq!((i * 2) as u8, vec_2[i]);
    }
}

#[test]
fn test_long_multiple_allocate() {
    unsafe {
        let mut raw1 = allocate_vec::<i64>(10i64);
        let mut raw2 = allocate_vec::<i64>(0i64);
        let mut vec_1 = into::<i64>(raw1.as_mut());
        let mut vec_2 = into::<i64>(raw2.as_mut());

        mem::forget(raw1);
        mem::forget(raw2);

        for i in 0..vec_1.len() {
            println!("initial value: {:?}", vec_1);
        }
        for i in 0..vec_1.len() {
            vec_1[i] = i as i64;
        }
        for i in 0..vec_1.len() {
            vec_2[i] = 1024 + (i * 2) as i64;
        }

        for i in 0..vec_1.len() {
            assert_eq!(i as i64, vec_1[i]);
        }
        for i in 0..vec_1.len() {
            assert_eq!(1024 + (i * 2) as i64, vec_2[i]);
        }
    }
}

#[test]
#[should_panic]
fn test_convert_misaligned1() {
    let mut src = vec![0u8; 1023];
    unsafe {
        into::<i16>(src.as_mut_slice());
    }
}

#[test]
#[should_panic]
fn test_convert_misaligned2() {
    let mut src = vec![0u8; 1022];
    unsafe {
        into::<i32>(src.as_mut_slice());
    }
}

    #[test]
    #[should_panic]
    fn test_convert_misaligned3() {
        let mut src = vec![0u8; 1020];
        unsafe {
            into::<i64>(src.as_mut_slice());
        }
    }
}