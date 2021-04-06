extern crate test;

mod test_simd {
    use super::test::Bencher;
    use rand::Rng;

    #[bench]
    pub fn test_min_max_vector_simd1(bencher: &mut Bencher) {
        let mut rng = rand::thread_rng();

        let count = 100;
        let groups = 2;
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
        let mut r_sum = vec![0i32; groups as usize];

        bencher.iter(|| {
            // dbg!(&v1);
            // dbg!(&v2);

            dbg!("starting a new iteration...........");
                let mut min = i32::max_value();
                let parts = 1.max(v1.len() / 8); // 8 == # of threads, minimum 1 in each part
                dbg!(v1.len(), parts);
                dbg!(v1.chunks(parts));
        });
        println!("values {} {} {} {} {} {:?}", min, max, sum, count, average, r_sum);
    }
}