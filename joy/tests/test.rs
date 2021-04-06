use std::collections::HashMap;

pub fn test(columns : &Vec<(i32, i32)>) {
    //let mut groups = HashMap::<i64, Vec<(i64, i64)>, BuildHasherDefault<XXHasher>>::default();
    let mut groups = HashMap::new();

    //tuple as a time processing
    //println!("creating groups based on hash");
    for x in columns {
        let group = groups.entry(x.0).or_insert(Vec::new());
        group.push(*x);
    }

    for x in groups.iter() {
        let mut sum :i32 = 0;
        for y in x.1 {
            sum = sum.wrapping_add(y.1);
        }
    }
}
