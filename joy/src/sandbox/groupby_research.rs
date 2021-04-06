///generate code for group by sum
extern crate test;

use std::cmp;

use rand::Rng;

///
/// gfn -> the function to create the group id, which is a hash
/// potential optimization:
///   - if input is ordered, we can use identity hash
///   - if input cardinality is small, we can use identify hash
///   - dynamically run in parallel depends on the system load, system core numbers
///
/// By default group by all columns now
///

pub fn groupby_i32_hash(rows: &Vec<(i32, i32, i32, i32, i32, i32, i32)>) -> (i32, i32) {
        let mut min = i32::min_value();
        let mut max = i32::max_value();
        let mut max = 0;
        for row in rows {
            min = cmp::min(min, row.0);
            max = cmp::max(max, row.0);
        }
        (min, max)
    //println!("hash duration: {:?}", t);

    // let t2 = time::Duration::span(|| {
    //     groups.par_iter().map(|x| )
    // });
    // println!("agg duration: {:?}", t2);
}

/// how to optimize calculations on the same column?? what requirements do we need to
/// support detection on this?
pub fn groupby_i32_identity_array(rows: &Vec<(i32, i32, i32, i32, i32, i32, i32)>) {
    let mut result1 = vec![0i32; 1000000];
    let mut result2 = vec![0i32; 1000000];
    //tuple as a time processing
    //println!("creating groups based on hash");
    unsafe {
        for row in rows {
            let idx = row.0 as usize;
            let agg1 = result1.get_unchecked_mut(idx);
            *agg1 = agg1.wrapping_add(row.1);
            let agg2 = result2.get_unchecked_mut(idx);
            *agg2 = agg2.wrapping_add(row.1);
        }
    }
    // for x in columns {
    //     result3[x.0 as usize] = result3[x.0 as usize].wrapping_add(x.3.wrapping_div(3));
    // }
    // for x in columns {
    //     result4[x.0 as usize] = result4[x.0 as usize].wrapping_add(x.4.wrapping_div(3));
    // }
}

pub fn generate_2_long_columns_i64() -> Vec<(i64, i64)> {
    let mut rng = rand::thread_rng();

    let count = 100_000_00;
    let mut result = Vec::new();
    for _i in 0..count {
        let tuple = (rng.gen_range(0..100000) as i64, rng.gen_range(1..1000) as i64);
        result.push(tuple);
    }

    println!("columns generated..........");
    return result;
}

pub fn generate_2_long_columns_i32() -> Vec<(i32, i32, i32, i32, i32, i32, i32)> {
    let mut rng = rand::thread_rng();

    let count = 1_000_000_00;
    let mut result = Vec::new();
    for _i in 0..count {
        let tuple = (rng.gen_range(0..1000000) as i32,
                     rng.gen_range(1..100) as i32,
                     rng.gen_range(1..100) as i32,
                     rng.gen_range(1..100) as i32, 0, 0, 0);
        result.push(tuple);
    }

    println!("columns generated.......... {}", result.len());
    return result;
}

#[cfg(test)]
mod tests_2 {
    use test::Bencher;

    use super::*;

    #[bench]
    fn test_groupy_agg_tuple_identity(bencher: &mut Bencher) {
        let columns = generate_2_long_columns_i32();
        bencher.iter(|| groupby_i32_identity_array(&columns));
    }

    #[bench]
    fn test_groupy_agg_tuple_hash(bencher: &mut Bencher) {
        let columns = generate_2_long_columns_i32();
        let result = bencher.iter(|| groupby_i32_hash(&columns));
        println!("result: {:?}", result);
    }

    #[bench]
    fn test_min_max_vector(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100000000;
        let mut v1 = vec![0i32; count];
        let mut v2 = vec![0i32; count];
        let mut v3 = vec![0i32; count];
        let mut v4 = vec![0i32; count];

        for i in 0..count {
            let j = i as usize;
            v1[j] = rng.gen_range(0..10000);
            v2[j] = rng.gen_range(0..10000);
            v3[j] = rng.gen_range(0..10000);
            v4[j] = rng.gen_range(0..10000);
        }
        let mut r_min = vec![0i32; 10000];
        let mut r_max = vec![0i32; 10000];
        let mut r_sum = vec![0i32; 10000];
        let mut r_count = vec![0i32; 10000];
        let mut r_average = vec![0i32; 10000];

        let mut min = i32::min_value();
        let mut max = i32::max_value();
        let mut sum = 0;
        let mut count = 0;
        let mut average = 0;
        bencher.iter(|| {
                         for i in 0..v1.len() {
                             let idx = v1[i] as usize;
                             r_min[idx] = cmp::min(r_min[idx], v2[i]);
                             r_max[idx] = cmp::max(r_max[idx], v3[i]);
                             r_sum[idx] += v4[i];
                             r_count[idx] +=1;
                             r_average[idx] = r_sum[idx]/r_count[idx];
                         }
                     });

        println!("values {} {} {} {} {}", min, max, sum, count, average);
    }

    #[bench]
    pub fn test_min_max_vector_simple(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100000000;
        let mut v1 = vec![0i32; count];
        let mut v2 = vec![0i32; count];
        let mut v3 = vec![0i32; count];
        let mut v4 = vec![0i32; count];

        for i in 0..count {
            let j = i as usize;
            v1[j] = rng.gen_range(0..10000);
            v2[j] = rng.gen_range(0..10000);
            v3[j] = rng.gen_range(0..10000);
            v4[j] = rng.gen_range(0..10000);
        }

        let mut min = i32::min_value();
        let mut max = i32::max_value();
        let mut sum = 0;
        let mut count = 1;
        let mut average = 0;

        bencher.iter(|| {
            for a in &v1 {
                min = cmp::min(min, *a);
                max = cmp::max(max, *a);
                sum += *a;
                count += 1;
                average = sum / 11;
            }
        });
        println!("values {} {} {} {} {}", min, max, sum, count, average);

    }

    #[bench]
    pub fn test_min_max_vector_simple2(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100000000;
        let mut v1 = vec![0i32; count];
        let mut v2 = vec![0i32; count];
        let mut v3 = vec![0i32; count];
        let mut v4 = vec![0i32; count];

        for i in 0..count {
            let j = i as usize;
            v1[j] = rng.gen_range(0..10000);
            v2[j] = rng.gen_range(0..10000);
            v3[j] = rng.gen_range(0..10000);
            v4[j] = rng.gen_range(0..10000);
        }

        let mut min = i32::min_value();
        let mut max = i32::max_value();
        let mut sum = 0;
        let mut count = 1;
        let mut average = 0;

        bencher.iter(|| {
            for i in 0..v1.len() {
                min = cmp::min(min, v1[i]);
                max = cmp::max(max, v1[i]);
                sum += v1[i];
                count += 1;
                average = sum / 11;
            }
        });
        println!("values {} {} {} {} {}", min, max, sum, count, average);

    }

    #[bench]
    pub fn test_min_max_vector_simple3(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100000000;
        let groups = 1000000;
        let mut v1 = vec![0i32; count];
        let mut v2 = vec![0i32; count];
        let mut v3 = vec![0i32; count];
        let mut v4 = vec![0i32; count];

        for i in 0..count {
            let j = i as usize;
            v1[j] = rng.gen_range(0..groups);
            v2[j] = rng.gen_range(0..groups);
            v3[j] = rng.gen_range(0..groups);
            v4[j] = rng.gen_range(0..groups);
        }

        let mut min = i32::min_value();
        let mut max = i32::max_value();
        let mut sum = 0;
        let mut count = 1;
        let mut average = 0;
        let mut r_min = vec![0i32; groups as usize];
        let mut r_max = vec![0i32; groups as usize];

        bencher.iter(|| {
            for i in 0..v1.len() {
                //separate this into 4 lanes and compute each aggregation batch?
                //data coming from different palce, can we use "mask"?
                let idx = v1[i] as usize;
                r_min[idx] = cmp::min(r_min[idx], v2[i]);
                //r_max[idx] = cmp::max(r_max[idx], v3[i]);

            }
        });
        println!("values {} {} {} {} {}", min, max, sum, count, average);

    }

    #[bench]
    pub fn test_min_max_vector_simple4(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100000000;
        let groups = 1000000;
        let mut v1 = vec![0i32; count];
        let mut v2 = vec![0i32; count];
        let mut v3 = vec![0i32; count];
        let mut v4 = vec![0i32; count];

        for i in 0..count {
            let j = i as usize;
            v1[j] = rng.gen_range(0..groups);
            v2[j] = rng.gen_range(0..groups);
            v3[j] = rng.gen_range(0..groups);
            v4[j] = rng.gen_range(0..groups);
        }

        let min = i32::min_value();
        let max = i32::max_value();
        let sum = 0;
        let count = 1;
        let average = 0;
        let mut r_min = vec![0i32; groups as usize];
        let r_max = vec![0i32; groups as usize];

        bencher.iter(|| {
            unsafe {
                for i in 0..v1.len() {
                    //separate this into 4 lanes and compute each aggregation batch?
                    //data coming from different palce, can we use "mask"?
                    let idx = v1[i] as usize;
                    let agg = r_min.get_unchecked_mut(idx);
                    *agg = cmp::min(*agg, v2[i]);
                    //r_max[idx] = cmp::max(r_max[idx], v3[i]);
                }
            }
        });
        println!("values {} {} {} {} {}", min, max, sum, count, average);

    }
}