extern crate test;

use rand::Rng;

/// ONLY USEFUL WITH SINGLE COLUMN AGGREGATION
pub fn groupby_i32_identity_array_columnar_input_loop_fusion(columns: &Vec<Vec<i32>>) -> Vec<Vec<i32>> {
    let group_count = 1000000;
    let mut result1 = vec![0i32; group_count];
    let mut result2 = vec![0i32; group_count];
    let mut result3 = vec![0i32; group_count];
    //tuple as a time processing
    //println!("creating groups based on hash");

    let column1 = columns.get(1).expect("error");
    let column2 = columns.get(2).expect("error");
    let column3 = columns.get(3).expect("error");
    unsafe {
        for x in columns.get(0) { //GROUP BY COLUMN 0
            for i in 0..x.len() {
                let group_value = x[i] as usize;
                let agg1 = result1.get_unchecked_mut(group_value);
                let agg2 = result2.get_unchecked_mut(group_value);
                let agg3 = result3.get_unchecked_mut(group_value);

                *agg1 = agg1.wrapping_add(*column1.get_unchecked(i));
                *agg2 = agg2.wrapping_add(*column2.get_unchecked(i));
                *agg3 = agg3.wrapping_add(*column3.get_unchecked(i));
            }
        }
    }
    let mut result = Vec::new();
    result.push(result1);
    result.push(result2);
    result.push(result3);
    result
}

pub fn groupby_i32_identity_array_columnar_input_loop_fusion_nounchecked(columns: &Vec<Vec<i32>>) -> Vec<Vec<i32>> {
    let group_count = 1000000;
    let mut result1 = vec![0i32; group_count];
    let mut result2 = vec![0i32; group_count];
    let mut result3 = vec![0i32; group_count];
    //tuple as a time processing
    //println!("creating groups based on hash");

    let column1 = columns.get(1).expect("error");
    let column2 = columns.get(2).expect("error");
    let column3 = columns.get(3).expect("error");
        for x in columns.get(0) { //GROUP BY COLUMN 0
            for i in 0..x.len() {
                let group_value = x[i] as usize;
                result1[group_value] = result1[group_value] + column1[i];
                result2[group_value] = result2[group_value] + column2[i];
                result3[group_value] = result3[group_value] + column3[i];
            }
        }
    let mut result = Vec::new();
    result.push(result1);
    result.push(result2);
    result.push(result3);
    result
}

/// ONLY USEFUL WITH SINGLE COLUMN AGGREGATION
pub fn groupby_i32_identity_array_columnar_input(columns: &Vec<Vec<i32>>) -> Vec<Vec<i32>> {
    let group_count = 1000000;
    let mut result1 = vec![0i32; group_count];
    let mut result2 = vec![0i32; group_count];
    let mut result3 = vec![0i32; group_count];
    //tuple as a time processing
    //println!("creating groups based on hash");

    let column1 = columns.get(1).expect("error");
    let column2 = columns.get(2).expect("error");
    let column3 = columns.get(3).expect("error");
        for x in columns.get(0) { //GROUP BY COLUMN 0
            for i in 0..x.len() {
                let group_value = x[i] as usize;
                result1[group_value] = result1[group_value] + column1[i];
            }
            for i in 0..x.len() {
                let group_value = x[i] as usize;
                result2[group_value] = result2[group_value] + column2[i];
            }
            for i in 0..x.len() {
                let group_value = x[i] as usize;
                result3[group_value] = result3[group_value] + column3[i];
            }
        }
    let mut result = Vec::new();
    result.push(result1);
    result.push(result2);
    result.push(result3);
    result
}


pub fn groupby_i32_hash_columnar_input(columns: &Vec<Vec<i32>>) -> Vec<Vec<i32>> {
    let group_count = 1000000;
    let mut result = vec![vec![0i32, 3]; group_count];
    //tuple as a time processing
    //println!("creating groups based on hash");
    let column1 = columns.get(1).expect("error");
    let column2 = columns.get(2).expect("error");
    let column3 = columns.get(3).expect("error");

    for x in columns.get(0) { //GROUP BY COLUMN 0
        for i in 0..x.len() {
            let mut group = &mut result[x[i] as usize];
            group[0] = group[0].wrapping_add(column1[i]);
            group[1] = group[1].wrapping_add(column2[i]);
            group[2] = group[2].wrapping_add(column3[i]);
        }
    }
    result
}

pub unsafe fn groupby_i32_identity_array_row_input(rows: &Vec<Vec<i32>>) -> Vec<Vec<i32>> {
    let group_count = 1000000;
    let mut result1 = vec![0i32; group_count];
    let mut result2 = vec![0i32; group_count];
    let mut result3 = vec![0i32; group_count];
    //tuple as a time processing
    //println!("creating groups based on hash");

        for row in rows {
            let group_value = *row.get_unchecked(0) as usize; //group by column 1
            result1[group_value] = result1[group_value] + row[1];
            result2[group_value] = result2[group_value] + row[2];
            result3[group_value] = result3[group_value] + row[3];
        }

    let mut result = Vec::new();
    result.push(result1);
    result.push(result2);
    result.push(result3);
    result
}

fn generate_long_row_i32(row_count: i32, col_count: i32, group_count: i32) -> Vec<Vec<i32>> {
    let mut rng = rand::thread_rng();

    let mut rows = Vec::new();
    for _ in 0..row_count {
        let mut col = Vec::new();
        for _ in 0..col_count {
            col.push(rng.gen_range(0..group_count) as i32);
        }
        rows.push(col);
    }

    println!("columnar columns: {} rows: {} group count {}", rows[0].len(), rows.len(), group_count);
    return rows;
}

fn generate_long_columar_i32(row_count: i32, col_count: i32, group_count: i32) -> Vec<Vec<i32>> {
    let mut rng = rand::thread_rng();

    let mut rows = Vec::new();
    for _ in 0..col_count {
        let mut col = Vec::new();
        for _ in 0..row_count {
            col.push(rng.gen_range(0..group_count) as i32);
        }
        rows.push(col);
    }

    println!("columnar columns: {} rows: {} group count {}", rows.len(), rows[0].len(), group_count);
    return rows;
}

pub fn groupby_appy(data: &Vec<Vec<i32>>) {
    //create all of the groups first

    // let mut groups = HashMap::new();
    // iter.map(|&a| &groups.insert(a, ))
    // for x in iter {
    //     groups.insert(x[0], x);
    // }
    // println!("groups : {:?}", groups);

    //apply the aggregation
}

#[cfg(test)]
mod groupby_col {
    use test::Bencher;

    use crate::groupby_col::generate_long_columar_i32;

    use super::*;

    static GROUP_COUNT: i32 = 1_0_000;
    static ROW_COUNT: i32 = 100_00_000;

    #[bench]
    fn test_groupy_agg_row(bencher: &mut Bencher) {
        let columns = generate_long_row_i32(ROW_COUNT, 4, GROUP_COUNT);
        unsafe {
            bencher.iter(|| groupby_i32_identity_array_row_input(&columns));
        }
    }

    #[bench]
    fn test_groupy_agg_columnar_fusion(bencher: &mut Bencher) {
        let columns = generate_long_columar_i32(ROW_COUNT, 4, GROUP_COUNT);
        bencher.iter(|| groupby_i32_identity_array_columnar_input_loop_fusion(&columns));
    }

    #[bench]
    fn test_groupy_agg_columnar_nofusion(bencher: &mut Bencher) {
        let columns = generate_long_columar_i32(ROW_COUNT, 4, GROUP_COUNT);
        bencher.iter(|| groupby_i32_identity_array_columnar_input(&columns));
    }

    #[bench]
    fn test_groupy_agg_columnar_nofusion_nounchecked(bencher: &mut Bencher) {
        let columns = generate_long_columar_i32(ROW_COUNT, 4, GROUP_COUNT);
        bencher.iter(|| groupby_i32_identity_array_columnar_input_loop_fusion_nounchecked(&columns));
    }

    #[test]
    fn test_correctness_fusion_no_unchecked() {
        let input = get_correctness_test_input();
            let result = groupby_i32_identity_array_columnar_input_loop_fusion_nounchecked(&input);
            verify_correctness_test_output(&result);
    }
    #[test]
    fn test_correctness_no_loop_fusion() {
        let input = get_correctness_test_input();
            let result = groupby_i32_identity_array_columnar_input(&input);
            verify_correctness_test_output(&result);
    }

    #[test]
    fn test_correctness_fusion_unchecked() {
        let input = get_correctness_test_input();
            let result = groupby_i32_identity_array_columnar_input_loop_fusion(&input);
            verify_correctness_test_output(&result)
    }

    #[test]
    fn test_correctness_groupby_apply() {
        let input = get_correctness_test_input();
        groupby_appy(&input);
    }

    fn  get_correctness_test_input() -> Vec<Vec<i32>> {
        let c1 = vec![1, 2, 1, 2, 1, 3, 1];
        let c2 = vec![2, 2, 2, 2, 2, 2, 2];
        let c3 = vec![3, 3, 3, 3, 3, 3, 3];
        let c4 = vec![4, 4, 4, 4, 4, 4, 4];

        let rows = vec![c1, c2, c3, c4];

        println!("input: {:?}", rows);
        rows
    }

    fn verify_correctness_test_output(result: &Vec<Vec<i32>>) {
        //[agg_result /** 0 indexed **/][key_value]
        assert_eq!(result[0][1], 8);
        assert_eq!(result[1][1], 12);
        assert_eq!(result[2][1], 16);
        assert_eq!(result[0][2], 4);
        assert_eq!(result[1][2], 6);
        assert_eq!(result[2][2], 8);
        assert_eq!(result[0][3], 2);
        assert_eq!(result[1][3], 3);
        assert_eq!(result[2][3], 4);
    }
}