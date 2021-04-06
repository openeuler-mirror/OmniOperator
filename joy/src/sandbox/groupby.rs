///generate code for group by sum
extern crate test;

#[cfg(test)]
mod tests_2 {
    use super::*;
    use test::Bencher;

    #[test]
    fn test_i32() {
        let c1 = vec![0i32, 10];
        let c2 = vec![0i32, 10];
        let c3 = vec![0i32, 10];

        let mut data = vec![c1, c2, c3];
        let groups = [0i32]; //group by the first column, 0-based index
        let agg = |a: i32| -> i32 {
            let mut result = 0;
            result += a;
            result
        };

        //data = groupby(data);
        println!("data = {:?}", data);
    }
    #[test]
    fn test_i64() {
        let c1 = vec![0i64, 10];
        let c2 = vec![0i64, 10];
        let c3 = vec![0i64, 10];

        let mut data = vec![c1, c2, c3];

        //data = groupby(data);
        println!("data = {:?}", data);
    }
}
