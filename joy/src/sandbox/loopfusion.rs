extern crate test;

#[cfg(test)]
mod loop_fusion {
    use super::*;
    use test::Bencher;


    #[bench]
    pub fn disabled(bencher: &mut Bencher) {
        let vec1 =vec![10; 10_000_000];
        let mut sum1: i32 = 0;
        let mut sum2: i32 = 0;
        bencher.iter(|| {
            for i in 0..vec1.len() {
                sum1 += vec1[i];
            }
            for i in 0..vec1.len() {
                sum2 += vec1[i]*2/3;
            }
        });
    }

    #[bench]
    pub fn disabled_with_wrapping(bencher: &mut Bencher) {
        let vec1 =vec![10; 10_000_000];
        let mut sum1: i32 = 0;
        let mut sum2: i32 = 0;
        bencher.iter(|| {
            for i in 0..vec1.len() {
                sum1 = sum1.wrapping_add(vec1[i]);
            }
            for i in 0..vec1.len() {
                sum2 = sum2.wrapping_add(vec1[i].wrapping_mul(2/3));
            }
        });
    }

    #[bench]
    fn disabled_blackbox(bencher: &mut Bencher) {
        let vec1 =test::black_box(vec![10; 10_000_000]);
        let mut sum1 = 0;
        let mut sum2 = 0;
        bencher.iter( || {
            for i in 0..vec1.len() {
                sum1 += vec1[i];
            }
            for i in 0..vec1.len() {
                sum2 += vec1[i]*2/3;
            }
        });
    }
    #[bench]
    fn enabled(bencher: &mut Bencher) {
        let vec1 =vec![10; 10_000_000];
        let mut sum1 = 0i32;
        let mut sum2 = 0i32;

        bencher.iter(||
        for i in 0..vec1.len() {
            sum1 = sum1.wrapping_add(vec1[i]);
            sum2 = sum2.wrapping_add(vec1[i]).wrapping_mul(2/3);
        });
    }


}