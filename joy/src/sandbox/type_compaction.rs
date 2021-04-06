extern crate test;

///type compaction only work if we read the data into the right type at
/// the BEGINNING of the query, e.g. table scan
/// WE CAN HAVE A FILED INDICATING WHETHER IT'S TYPE COMPACTED
#[cfg(test)]
mod type_compaction {
    use super::*;
    use test::Bencher;

    #[bench]
    fn disabled_float64(bencher: &mut Bencher) {
        let vec1 =vec![10f64; 1_000_000];
        let mut sum = 0f64;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn disabled_float64_blackbox(bencher: &mut Bencher) {
        let vec1 = test::black_box(vec![10f64; 1_000_000]);
        let mut sum = 0f64;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn disabled_float32(bencher: &mut Bencher) {
        let vec1 =vec![10f32; 1_000_000];
        let mut sum = 0f32;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn disabled_float32_blackbox(bencher: &mut Bencher) {
        let vec1 = test::black_box(vec![10f32; 1_000_000]);
        let mut sum = 0f32;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn disabled_i32(bencher: &mut Bencher) {
        let vec1 =vec![10i32; 1_000_000];
        let mut sum = 0i32;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn disabled_i32_blackbox(bencher: &mut Bencher) {
        let vec1 = test::black_box(vec![10i32; 1_000_000]);
        let mut sum = 0i32;
        bencher.iter(||
            for i in 0..vec1.len() {
                sum += vec1[i];
            });
    }

    #[bench]
    fn enabled_withcopy(bencher: &mut Bencher) {
        let mut vec1 =vec![10u8; 1_000_000];
        let vec_orig =vec![10f32; 1_000_000];

        let mut sum = 0;

        bencher.iter(||
            {
                for i in 0..vec_orig.len() {
                    vec1[i] = vec_orig[i] as u8;
                }
                for i in 0..vec1.len() {
                    sum += vec1[i];
                }
            });
    }

    #[bench]
    fn enabled_nocopy(bencher: &mut Bencher) {
        let vec1 =vec![10u8; 1_000_000];

        let mut sum = 0;

        bencher.iter(||
            {
                for i in 0..vec1.len() {
                    sum += vec1[i];
                }
            });
    }
}